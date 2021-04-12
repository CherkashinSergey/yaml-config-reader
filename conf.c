/*
 * Example libyaml parser.
 *
 * This is a basic example to demonstrate how to convert yaml to raw data
 * using the libyaml emitter API. Example yaml data to be parsed:
 *
 *    $ cat fruit.yaml
 *    ---
 *    fruit:
 *    - name: apple
 *      color: red
 *      count: 12
 *    - name: orange
 *      color: orange
 *      count: 3
 *    - name: bannana
 *      color: yellow
 *      count: 4
 *    - name: mango
 *      color: green
 *      count: 1
 *    ...
 *
 *    $ ./parse < fruit.yaml
 *    data[0]={name=apple, color=red, count=12}
 *    data[1]={name=orange, color=orange, count=3}
 *    data[2]={name=bannana, color=yellow, count=4}
 *    data[3]={name=mango, color=green, count=1}
 */
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
enum state_value {
    WAIT_DIRECTIVE,
    ACCEPT_KEY,
    ACCEPT_VALUE
};

enum parser_stage {
    START,
    ACCEPT_PIPELINE,
    FINISH_PIPELINE,
    ACCEPT_PIPELINE_NAME,
    ACCEPT_PIPELINE_TYPE,
    ACCEPT_TRANSFORM,
    ACCEPT_TRANSFORM_WORKERS,
    ACCEPT_TRANSFORM_BUFFER,
    ACCEPT_TRANSFORM_BUFFER_SIZE,
    ACCEPT_TRANSFORM_BUFFER_EVENTS,
    FINISH_TRANSFORM_BUFFER,
    FINISH_TRANSFORM,
    STOP,
    ERROR
};

struct parser_state {
    enum state_value state;
    enum parser_stage stage;
    int accepted;
    int error;
    char *key;
    char *value;
    pipe_config data;
};

int setPipeline(struct parser_state *s, yaml_event_t *event)
{
    char *key = NULL;
    if (s->state == ACCEPT_KEY){
        switch (event->type) {
        case YAML_SCALAR_EVENT:
            key = (char*)event->data.scalar.value;
            if (strcmp(key, "pipeline_name") == 0) {
                s->stage = ACCEPT_PIPELINE_NAME;
                s->state = ACCEPT_VALUE;
            } else if (strcmp(key, "pipeline_type") == 0) {
                s->stage = ACCEPT_PIPELINE_TYPE;
                s->state = ACCEPT_VALUE;
            } else if (strcmp(key, "transform") == 0) {
                s->stage = ACCEPT_TRANSFORM;
                s->state = ACCEPT_KEY;
            } else {
                fprintf(stderr, "Unknown key: %s\n", s->key);
                s->stage = ERROR;
            }
            break;
        case YAML_MAPPING_END_EVENT:
            /* Pileline options are finished. */
            s->state = FINISH_PIPELINE;
            s->accepted = 1;
            break;
        default:
            fprintf(stderr, "Unexpected event while getting key: %d\n",
                    event->type);
            s->state = ERROR;
            break;
        }
    }
    else if (s->state == ACCEPT_VALUE){
        if (event->type != YAML_SCALAR_EVENT) {
            fprintf(stderr, "Unexpected event while getting value: %d\n",
                    event->type);
            s->state = ERROR;
        } else {
            s->value = (char*)event->data.scalar.value;
            switch (s->stage)
            {
            case ACCEPT_PIPELINE_NAME:
                s->data.name = strdup((char*)s->value);
                break;
            case ACCEPT_PIPELINE_TYPE:
                s->data.type = strdup((char*)s->value);
                break;
            default:
                break;
            }
            s->state = ACCEPT_KEY;
        }
    }
}

int setTransform(struct parser_state *s, yaml_event_t *event)
{
    if (s->state == ACCEPT_KEY){
        char *key = NULL;
        switch (event->type) {
        case YAML_SCALAR_EVENT:
            key = (char*)event->data.scalar.value;
            if (s->stage == ACCEPT_TRANSFORM && strcmp(key, "workers") == 0) {
                s->stage = ACCEPT_TRANSFORM_WORKERS;
                s->state = ACCEPT_VALUE;
            } else if (s->stage == ACCEPT_TRANSFORM && strcmp(key, "buffer") == 0) {
                s->stage = ACCEPT_TRANSFORM_BUFFER;
            } else if (s->stage == ACCEPT_TRANSFORM_BUFFER && strcmp(key, "size") == 0) {
                s->stage = ACCEPT_TRANSFORM_BUFFER_SIZE;
                s->state = ACCEPT_VALUE;
            } else if (s->stage == ACCEPT_TRANSFORM_BUFFER && strcmp(key, "events") == 0) {
                s->stage = ACCEPT_TRANSFORM_BUFFER_EVENTS;
                s->state = ACCEPT_VALUE;
            } else {
                fprintf(stderr, "Unknown key: %s\n", s->key);
                s->stage = ERROR;
            }
            break;
        case YAML_MAPPING_START_EVENT:
            /* Transform options are started. */
            if (s->stage != ACCEPT_TRANSFORM &&
                s->stage != ACCEPT_TRANSFORM_BUFFER) {
                    fprintf(stderr, "Unexpected mapping event while getting 'transform' params: %d\n",
                    event->type);
                    s->stage = ERROR;
                }
            break;
        case YAML_MAPPING_END_EVENT:
            /* Pileline options are finished. */
            switch (s->stage) {
            case ACCEPT_TRANSFORM_BUFFER:
                s->stage = ACCEPT_TRANSFORM;
                break;
            case ACCEPT_TRANSFORM:
                s->stage = ACCEPT_PIPELINE;
                break;
            default:
                fprintf(stderr, "Unexpected mapping_end event while getting 'transform' params: %d\n",
                    event->type);
                s->stage = ERROR;
                break;
            }
            break;
        default:
            fprintf(stderr, "Unexpected event while getting key: %d\n",
                    event->type);
            s->state = ERROR;
            break;
        }
    }
    else if (s->state == ACCEPT_VALUE){
        if (event->type != YAML_SCALAR_EVENT) {
            fprintf(stderr, "Unexpected event while getting value: %d\n",
                    event->type);
            s->state = ERROR;
        } else {
            char *value = (char*)event->data.scalar.value;
            switch (s->stage)
            {
            case ACCEPT_TRANSFORM_WORKERS:
                s->data.transform.workers_count = atoi(s->value);
                s->stage = ACCEPT_TRANSFORM;
                break;
            case ACCEPT_TRANSFORM_BUFFER_EVENTS:
                s->data.transform.buffer.events_count = atoi(s->value);
                s->stage = ACCEPT_TRANSFORM_BUFFER;
                break;
            case ACCEPT_TRANSFORM_BUFFER_SIZE:
                s->data.transform.buffer.size = atoi(s->value);
                s->stage = ACCEPT_TRANSFORM_BUFFER;
                break;
            default:
                break;
            }
            s->state = ACCEPT_KEY;
        }
    }
}

int consume_event(struct parser_state *s, yaml_event_t *event)
{
    s->accepted = 0;
    switch (s->stage) {
    case START:
    case FINISH_PIPELINE:
        switch (event->type) {
        case YAML_STREAM_START_EVENT:
            /* Parser initialized. */
        case YAML_DOCUMENT_START_EVENT:
            /* Document opened. */
        case YAML_SEQUENCE_START_EVENT:
            /* List of pipelines started. */
            s->state = WAIT_DIRECTIVE;
            break;
        case YAML_MAPPING_START_EVENT:
            s->stage = ACCEPT_PIPELINE;
            /* TODO: allocate memory for new pipeline config. */
            break;
        case YAML_STREAM_END_EVENT:
            s->stage = STOP;
            break;
        default:
            break;
        }
        break;
    case ACCEPT_PIPELINE:
    case ACCEPT_PIPELINE_NAME:
    case ACCEPT_PIPELINE_TYPE:
        /* Pipeline config starts with key name. */
        if (s->state == WAIT_DIRECTIVE)
            s->state = ACCEPT_KEY;
        setPipeline(s,event);
        break;
    case ACCEPT_TRANSFORM:
    case ACCEPT_TRANSFORM_WORKERS:
    case ACCEPT_TRANSFORM_BUFFER:
    case ACCEPT_TRANSFORM_BUFFER_SIZE:
    case ACCEPT_TRANSFORM_BUFFER_EVENTS:
        /* Transform config starts with key name. */
        setTransform(s, event);
        break;
    case ERROR:
    case STOP:
        break;
    }

    return (s->state == ERROR ? 0 : 1);
}

int parse_config(char *config_path)
{
    yaml_parser_t parser;
    yaml_event_t event;
    struct parser_state state = {.stage=START, .state=WAIT_DIRECTIVE, .accepted=0, .error=0};
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
        if (!consume_event(&state, &event)) {
            goto error;
        }
        if (state.accepted && i < sizeof(data)/sizeof(*data)) {
            data[i].name = state.data.name;
            data[i].type = state.data.type;
            printf("data[%d]={name=%s, count=%s}\n",
                i, data[i].name, data[i].type);
            i++;
        }
        yaml_event_delete(&event);
    } while (state.stage != STOP);

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
