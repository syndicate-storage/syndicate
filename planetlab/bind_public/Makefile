all: bind_public test

bind_public:
	gcc -g -shared -fPIC -ldl -lrt bind_public.c -o bind_public.so

test:
	gcc -g test-bind.c -o test-bind

clean:
	rm -f bind_public.so test-bind
