#define _XOPEN_SOURCE /* Mac compatibility. */
#include <ucontext.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/mman.h>
#include <time.h>

static ucontext_t* contexts;
static ucontext_t uctx_main;
static int* prev;
static int* next;
static int** data;
static int* size;
static long long* all_times;
static int* number_of_chenges;

#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024

#define change_coroutine() ({						                                    \
    if (prev[id] != id) {                                                               \
        if (clock() * 1000000 / CLOCKS_PER_SEC - cur_time > target_latency * 1000) {    \
            sum_time += clock() * 1000000 / CLOCKS_PER_SEC - cur_time;                  \
            if (swapcontext(&contexts[id], &contexts[next[id]]) == -1) {                \
			    handle_error("swapcontext");                                            \
            }                                                                           \
            ++(*number_of_chenges);                                                     \
            cur_time = clock() * 1000000 / CLOCKS_PER_SEC;                              \
        }                                                                               \
    }                                                                                   \
})

static void
my_coroutine(int id, const char* filename, int** result_data, int* result_size, long long target_latency, long long* result_time, int* number_of_chenges)
{
    long long cur_time = clock() * 1000 / CLOCKS_PER_SEC, sum_time = 0;
    change_coroutine();
	printf("func%d, %s: started\n", id, filename);
    change_coroutine();
    FILE *fin;
    change_coroutine();
    fin = fopen(filename, "r");
    change_coroutine();
    int capacity = 1;
    change_coroutine();
    int size = 0;
    change_coroutine();
    int* data = (int*) malloc(capacity * sizeof(int));
    change_coroutine();
    while (fscanf(fin, "%d", &data[size]) == 1) {
        change_coroutine();
        ++size;
        change_coroutine();
        if (size == capacity) {
            change_coroutine();
            capacity *= 2;
            change_coroutine();
            int* temp = (int*) malloc(capacity * sizeof(int));
            change_coroutine();
            for (int i = 0; i < size; ++i) {
                change_coroutine();
                temp[i] = data[i];
                change_coroutine();
            }
            change_coroutine();
            free(data);
            change_coroutine();
            data = temp;
            change_coroutine();
        }
        change_coroutine();
    }
    change_coroutine();
    fclose(fin);
    change_coroutine();
	printf("func%d, %s: read completed, size of array: %d\n", id, filename, size);
    change_coroutine();
    *result_size = size;
    change_coroutine();
    *result_data = data;
    change_coroutine();

    int power = 1;
    change_coroutine();
    capacity = 1;
    change_coroutine();
    while (power < size) {
        change_coroutine();
        ++capacity;
        change_coroutine();
        power *= 2;
        change_coroutine();
    }
    change_coroutine();
    int* L = malloc(capacity * sizeof(int));
    change_coroutine();
    int* R = malloc(capacity * sizeof(int));
    change_coroutine();
    int* C = malloc(capacity * sizeof(int));
    change_coroutine();
    L[0] = 0;
    change_coroutine();
    R[0] = size;
    change_coroutine();
    C[0] = 0;
    change_coroutine();
    int top = 1;
    change_coroutine();
    while (top > 0) {
        change_coroutine();
        int l = L[top - 1];
        change_coroutine();
        int r = R[top - 1];
        change_coroutine();
        int c = C[top - 1];
        change_coroutine();
        if (l + 1 == r) {
            change_coroutine();
            --top;
            change_coroutine();
        } else if (c == 0) {
            change_coroutine();
            ++C[top - 1];
            change_coroutine();
            L[top] = l;
            change_coroutine();
            R[top] = (l + r) / 2;
            change_coroutine();
            C[top] = 0;
            change_coroutine();
            ++top;
            change_coroutine();
        } else if (c == 1) {
            change_coroutine();
            ++C[top - 1];
            change_coroutine();
            L[top] = (l + r) / 2;
            change_coroutine();
            R[top] = r;
            change_coroutine();
            C[top] = 0;
            change_coroutine();
            ++top;
            change_coroutine();
        } else {
            change_coroutine();
            size = r - l + 1;
            change_coroutine();
            int* temp = (int*) malloc(size * sizeof(int));
            change_coroutine();
            int j = l, k = (l + r) / 2;
            change_coroutine();
            for (int i = 0; i < size; ++i) {
                change_coroutine();
                if (k ==  r  || (j < (l + r) / 2 && data[j] < data[k])) {
                    change_coroutine();
                    temp[i] = data[j];
                    change_coroutine();
                    ++j;
                    change_coroutine();
                } else {
                    change_coroutine();
                    temp[i] = data[k];
                    change_coroutine();
                    ++k;
                    change_coroutine();
                }
                change_coroutine();
            }
            change_coroutine();
            for (int i = 0; i < size; ++i) {
                change_coroutine();
                data[l + i] = temp[i];
                change_coroutine();
            }
            change_coroutine();
            free(temp);
            change_coroutine();
            --top;
            change_coroutine();
        }
        change_coroutine();
    }
    change_coroutine();
	printf("func%d, %s: sort is completed\n", id, filename);

    change_coroutine();

    if (prev[id] == id) {
        if (id != 0) {
		    if (swapcontext(&contexts[id], &contexts[0]) == -1) {
			    handle_error("swapcontext");
            }
        }
    } else {
        next[prev[id]] = next[id];
        prev[next[id]] = prev[id];
		if (swapcontext(&contexts[id], &contexts[next[id]]) == -1) {
			handle_error("swapcontext");
        }
    }
    *result_time = sum_time;
	printf("func%d: returning\n", id);
}

static void *
allocate_stack()
{
	void *stack = malloc(stack_size);
	stack_t ss;
	ss.ss_sp = stack;
	ss.ss_size = stack_size;
	ss.ss_flags = 0;
	sigaltstack(&ss, NULL);
	return stack;
}

void merge(int l, int r, int** data, int* size) {
    if (l + 1 < r) {
        merge(l, (l + r) / 2, data, size);
        merge((l + r) / 2, r, data, size);
        r = (l + r) / 2;
        int* temp = malloc((size[l] + size[r]) * sizeof(int));
        int j = 0, k = 0;
        for (int i = 0; i < size[l] + size[r]; ++i) {
            if (k == size[r] || (j < size[l] && data[l][j] < data[r][k])) {
                temp[i] = data[l][j];
                ++j;
            } else {
                temp[i] = data[r][k];
                ++k;
            }
        }
        free(data[l]);
        free(data[r]);
        data[l] = temp;
        size[l] += size[r];
    }
}

int
main(int argc, char *argv[])
{
    int n = argc - 2;
    long long target_latency = atoi(argv[1]);
    contexts = malloc((n) * sizeof(ucontext_t));
    prev = malloc(n * sizeof(int));
    next = malloc(n * sizeof(int));
    data = malloc(n * sizeof(int*));
    size = malloc(n * sizeof(int));
    number_of_chenges = malloc(n * sizeof(int));
    all_times = malloc(n * sizeof(long long));
    for (int i = 0; i < n; ++i) {
		if (getcontext(&contexts[i]) == -1) {
			handle_error("getcontext");
        }

        contexts[i].uc_stack.ss_sp = allocate_stack();
        contexts[i].uc_stack.ss_size = stack_size;

        if (i + 1 == n) {
            contexts[i].uc_link = &uctx_main;
        } else {
		    contexts[i].uc_link = &contexts[i + 1];
        }

        prev[i] = (i - 1 + n) % n;
        next[i] = (i + 1) % n;

        makecontext(&contexts[i], my_coroutine, 7, i, argv[i + 2], &data[i], &size[i], target_latency, &all_times[i], &number_of_chenges[i]);
    }

	if (swapcontext(&uctx_main, &contexts[0]) == -1) {
		handle_error("swapcontext");
    }
    for (int i = 0; i < n; ++i) {
        printf("working times of corutine %d: %lldus, number of chenges: %d\n", i, all_times[i], number_of_chenges[i]);
    }

    printf("starting merging\n");
    merge(0, n, data, size);
    printf("merginfg completed, total size = %d\n", size[0]);

    FILE* fout = fopen("out.txt", "w");
    for (int i = 0; i < size[0] ; ++i) {
        fprintf(fout, "%d ", data[0][i]);
    }
    fclose(fout);

	printf("main: exiting\n");

	return 0;
}
