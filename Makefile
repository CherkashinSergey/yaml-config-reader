all: conf

conf: clean
	gcc -g -o conf -I ./libyaml/include -L ./libyaml/src/.libs/ ./conf.c -lyaml

fruit: clean
	gcc -g -o fruit -I ./libyaml/include -L ./libyaml/src/.libs/ ./fruit.c -lyaml

clean:
	rm -f fruit conf
