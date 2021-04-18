#include <yaml.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>



/* Pipe config structure */
typedef struct buffer_config {
    size_t size;
    size_t events_count;
} buffer_config;

typedef struct stage_config {
    int             workers_count;
    buffer_config   buffer;
} stage_config;

typedef struct pipe_config {
    char *name;
    char *type;

    stage_config transform;
    stage_config output;
} pipe_config;

/* Parser states. */
enum value_state {
    WAIT_DIRECTIVE,
    ACCEPT_KEY,
    ACCEPT_VALUE
};

enum pipe_config_state {
    CONFIG_ROOT,
    ACCEPT_PIPELINE,
    ACCEPT_PIPELINE_NAME,
    ACCEPT_PIPELINE_TYPE,
    ACCEPT_TRANSFORM,
    ACCEPT_OUTPUT,
    STOP,
    ERROR
};

enum stage_config_state {
    STAGE_ROOT,
    WORKERS,
    BUFFER,
    BUFFER_SIZE,
    BUFFER_EVENTS
};

union nested_config_state {
    enum stage_config_state stage;
};

struct parser_state {
    enum value_state value_state;
    enum pipe_config_state pipe_state;
    union nested_config_state nested_state;
    int accepted;
    int error;
    pipe_config *data;
};

int setPipeline(struct parser_state *s, yaml_event_t *event)
{
    char *key = NULL;
    if (s->value_state == ACCEPT_KEY){
        switch (event->type) {
        case YAML_SCALAR_EVENT:
            key = (char*)event->data.scalar.value;
            if (strcmp(key, "pipeline_name") == 0) {
                s->pipe_state = ACCEPT_PIPELINE_NAME;
                s->value_state = ACCEPT_VALUE;
            } else if (strcmp(key, "pipeline_type") == 0) {
                s->pipe_state = ACCEPT_PIPELINE_TYPE;
                s->value_state = ACCEPT_VALUE;
            } else if (strcmp(key, "transform") == 0) {
                s->pipe_state = ACCEPT_TRANSFORM;
                s->nested_state.stage = STAGE_ROOT;
                s->value_state = ACCEPT_KEY;
            } else if (strcmp(key, "output") == 0) {
                s->pipe_state = ACCEPT_OUTPUT;
                s->nested_state.stage = STAGE_ROOT;
                s->value_state = ACCEPT_KEY;
            } else {
                fprintf(stderr, "Unknown key: %s\n", key);
                s->pipe_state = ERROR;
            }
            break;
        case YAML_MAPPING_END_EVENT:
            /* Pileline options are finished. */
            s->value_state = CONFIG_ROOT;
            s->accepted = 1;
            break;
        default:
            fprintf(stderr, "Unexpected event while getting key: %d\n",
                    event->type);
            s->value_state = ERROR;
            break;
        }
    }
    else if (s->value_state == ACCEPT_VALUE){
        if (event->type != YAML_SCALAR_EVENT) {
            fprintf(stderr, "Unexpected event while getting value: %d\n",
                    event->type);
            s->value_state = ERROR;
        } else {
            char *value = (char*)event->data.scalar.value;
            switch (s->pipe_state)
            {
            case ACCEPT_PIPELINE_NAME:
                s->data->name = strdup(value);
                break;
            case ACCEPT_PIPELINE_TYPE:
                s->data->type = strdup(value);
                break;
            default:
                break;
            }
            s->value_state = ACCEPT_KEY;
        }
    }
}

int setStage(struct parser_state *s, yaml_event_t *event, stage_config *config)
{
    if (s->value_state == ACCEPT_KEY){
        char *key = NULL;
        switch (event->type) {
        case YAML_SCALAR_EVENT:
            key = (char*)event->data.scalar.value;
            if (s->nested_state.stage == STAGE_ROOT && strcmp(key, "workers") == 0) {
                s->nested_state.stage = WORKERS;
                s->value_state = ACCEPT_VALUE;
            } else if (s->nested_state.stage == STAGE_ROOT && strcmp(key, "buffer") == 0) {
                s->nested_state.stage = BUFFER;
            } else if (s->nested_state.stage == BUFFER &&
                       strcmp(key, "size") == 0) {
                s->nested_state.stage = BUFFER_SIZE;
                s->value_state = ACCEPT_VALUE;
            } else if (s->nested_state.stage == BUFFER && strcmp(key, "events") == 0) {
                s->nested_state.stage = BUFFER_EVENTS;
                s->value_state = ACCEPT_VALUE;
            } else {
                fprintf(stderr, "Unknown key: %s\n", key);
                s->pipe_state = ERROR;
            }
            break;
        case YAML_MAPPING_START_EVENT:
            /* Transform options are started. */
            if (s->pipe_state != ACCEPT_TRANSFORM && /* TODO: Do not check pipe_root states */
                s->pipe_state != ACCEPT_OUTPUT &&
                s->nested_state.stage != BUFFER) {
                    fprintf(stderr, "Unexpected mapping event while getting 'transform' params: %d\n",
                    event->type);
                    s->pipe_state = ERROR;
                }
            break;
        case YAML_MAPPING_END_EVENT:
            /* Nested options are finished. */
            switch (s->nested_state.stage) {
            case BUFFER:
                s->nested_state.stage = STAGE_ROOT;
                break;
            case STAGE_ROOT:
                /* Finished options of stage. */
                s->pipe_state = ACCEPT_PIPELINE;
                break;
            default:
                fprintf(stderr, "Unexpected mapping_end event while getting 'transform' params: %d\n",
                    event->type);
                s->pipe_state = ERROR;
                break;
            }
            break;
        default:
            fprintf(stderr, "Unexpected event while getting key: %d\n",
                    event->type);
            s->value_state = ERROR;
            break;
        }
    }
    else if (s->value_state == ACCEPT_VALUE){
        if (event->type != YAML_SCALAR_EVENT) {
            fprintf(stderr, "Unexpected event while getting value: %d\n",
                    event->type);
            s->value_state = ERROR;
        } else {
            char *value = (char*)event->data.scalar.value;
            switch (s->nested_state.stage)
            {
            case WORKERS:
                config->workers_count = atoi(value);
                s->nested_state.stage = STAGE_ROOT;
                break;
            case BUFFER_EVENTS:
                config->buffer.events_count = atoi(value);
                s->nested_state.stage = BUFFER;
                break;
            case BUFFER_SIZE:
                config->buffer.size = atoi(value);
                s->nested_state.stage = BUFFER;
                break;
            default:
                break;
            }
            s->value_state = ACCEPT_KEY;
        }
    }
}

int consume_event(struct parser_state *s, yaml_event_t *event)
{
    s->accepted = 0;
    switch (s->pipe_state) {
    case CONFIG_ROOT:
        switch (event->type) {
        case YAML_STREAM_START_EVENT:
            /* Parser initialized. */
        case YAML_DOCUMENT_START_EVENT:
            /* Document opened. */
        case YAML_SEQUENCE_START_EVENT:
            /* List of pipelines started. */
            s->value_state = WAIT_DIRECTIVE;
            break;
        case YAML_MAPPING_START_EVENT:
            s->pipe_state = ACCEPT_PIPELINE;
            s->data = malloc(sizeof(pipe_config));
            break;
        case YAML_STREAM_END_EVENT:
            s->pipe_state = STOP;
            break;
        default:
            break;
        }
        break;
    case ACCEPT_PIPELINE:
    case ACCEPT_PIPELINE_NAME:
    case ACCEPT_PIPELINE_TYPE:
        /* Pipeline config starts with key name. */
        if (s->value_state == WAIT_DIRECTIVE)
            s->value_state = ACCEPT_KEY;
        setPipeline(s,event);
        break;
    case ACCEPT_TRANSFORM:
        /* Transform config starts with key name. */
        setStage(s, event, &s->data->transform);
        break;
    case ACCEPT_OUTPUT:
        /* Transform config starts with key name. */
        setStage(s, event, &s->data->output);
        break;
    case ERROR:
    case STOP:
        break;
    }

    return (s->pipe_state == ERROR ? 0 : 1);
}

int print_config(pipe_config *cfg)
{
    if (!cfg){
        printf("cfg is NULL\n");
        return 1;
    }
    printf("pipe name: %s\n"
           "pipe type: %s\n"
           "transform:\n"
           "  workers: %d\n"
           "    buffer:\n"
           "      size: %ld\n"
           "      events: %ld\n"
           "output:\n"
           "  workers: %d\n"
           "  buffer:\n"
           "    size: %ld\n"
           "    events: %ld\n",
           cfg->name, cfg->type,
           cfg->transform.workers_count, cfg->transform.buffer.size, cfg->transform.buffer.events_count,
           cfg->output.workers_count, cfg->output.buffer.size, cfg->output.buffer.events_count
    );
    return 0;
}

int free_value_state(struct parser_state *ps)
{
    free(ps->data->name);
    free(ps->data->type);
    free(ps->data);
    memset(ps, 0, sizeof(struct parser_state));
}

int parse_config(char *config_path)
{
    yaml_parser_t parser;
    yaml_event_t event;
    struct parser_state value_state = {.pipe_state=CONFIG_ROOT, .value_state=WAIT_DIRECTIVE, .accepted=0, .error=0};
    pipe_config data[64];
    int i = 0;

    FILE *fptr = fopen(config_path,"r");

    memset(data, 0, sizeof(data));
    yaml_parser_initialize(&parser);
    yaml_parser_set_input_file(&parser, fptr);

    do {
        if (!yaml_parser_parse(&parser, &event)) {
            goto error;
        }
        if (!consume_event(&value_state, &event)) {
            goto error;
        }
        if (value_state.accepted && i < sizeof(data)/sizeof(*data)) {
            print_config(value_state.data);
            free_value_state(&value_state);
            i++;
        }
        yaml_event_delete(&event);
    } while (value_state.pipe_state != STOP);

    yaml_parser_delete(&parser);
    return 0;

error:
    yaml_parser_delete(&parser);
    return 1;
}

int main(int argc, char *argv[])
{
    parse_config("config.yml");

    return 0;
}
