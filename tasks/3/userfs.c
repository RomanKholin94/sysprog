#include "userfs.h"
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

#define min(x, y) ( (x) < (y) ? (x) : (y) )

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 1024,
};

/** Global error code. Set from any function on any error. */
static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char *memory;
	/** How many bytes are occupied. */
	int occupied;
	/** Next block in the file. */
	struct block *next;
	/** Previous block in the file. */
	struct block *prev;
};

struct file {
	/** Double-linked list of file blocks. */
	struct block *block_list;
	/**
	 * Last block in the list above for fast access to the end
	 * of file.
	 */
	struct block *last_block;
	/** How many file descriptors are opened on the file. */
	int refs;
	/** File name. */
	const char *name;
	/** Files are stored in a double-linked list. */
	struct file *next;
	struct file *prev;

    int size;

    int isDeleted;
};

/** List of all files. */
static struct file *file_list = NULL;

struct filedesc {
	struct file *file;

    struct block *block;

    int block_offset;

    int totall_offset;

    int flag;
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static struct filedesc **file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

struct file *
find_file(const char *filename)
{
    if (file_list) {
        struct file *file = file_list;
        while (file) {
            if (!file->isDeleted && strcmp(filename, file->name) == 0) {
                return file;
            }
            file = file->next;
        }
    } else {
        return 0;
    }
}

struct block *
make_block()
{
    struct block *block = malloc(sizeof(struct block));
	block->memory = malloc(sizeof(char) * BLOCK_SIZE);
	block->occupied = 0;
	block->next = NULL;
	block->prev = NULL;
    return block;
}

struct file *
make_file(const char *filename)
{
    struct file *file = malloc(sizeof(struct file));
    file->block_list = make_block();
    file->last_block = file->block_list;
    file->refs = 0;
    file->name = malloc(sizeof(char) * (strlen(filename) + 1));
    strcpy(file->name, filename);
    file->prev = NULL;
    file->next = NULL;
    file->size = 0;
    file->isDeleted = 0;
    return file;
}

struct file *
add_file(const char *filename)
{
    struct file *file = make_file(filename);
    if (file_list) {
        file->next = file_list;
        file_list->prev = file;
        file_list = file;
    } else {
        file_list = file;
    }
    return file;
}

struct filedesc *
make_filedesc(struct filedesc **filedesc_pointer, int flags, struct file *file)
{
    struct filedesc *filedesc = *filedesc_pointer = malloc(sizeof(struct filedesc));
    filedesc->file = file;
    ++(file->refs);
    filedesc->block = file->block_list;
    filedesc->block_offset = 0;
    if (flags & UFS_READ_WRITE) {
        filedesc->flag = UFS_READ_WRITE;
    } else if (flags & UFS_WRITE_ONLY) {
        filedesc->flag = UFS_WRITE_ONLY;
    } else if (flags & UFS_READ_ONLY) {
        filedesc->flag = UFS_READ_ONLY;
    } else {
        filedesc->flag = UFS_READ_WRITE;
    }
    filedesc->totall_offset = 0;
    return filedesc;
}

int
push_filedesc(struct file *file, int flags)
{
    for (int i = 0; i < file_descriptor_count; ++i) {
        if (file_descriptors[i] == NULL) {
            file_descriptors[i] = malloc(sizeof(struct filedesc));
            make_filedesc(&file_descriptors[i], flags, file);
            return i;
        }
    }
    if (file_descriptor_capacity) {
        if (file_descriptor_capacity <= file_descriptor_count * 2) {
            file_descriptor_capacity *= 2;
            struct filedesc **temp_file_descriptors = malloc(sizeof(struct filedesc *) * file_descriptor_capacity);
            for (int i = 0; i < file_descriptor_count; ++i) {
                temp_file_descriptors[i] = file_descriptors[i];
            }
            free(file_descriptors);
            file_descriptors = temp_file_descriptors;
        }
    } else {
        file_descriptor_capacity = 1;
        file_descriptors = malloc(sizeof(struct filedesc *));
    }
    make_filedesc(&file_descriptors[file_descriptor_count], flags, file);
    return file_descriptor_count++;
}

int
ufs_open(const char *filename, int flags)
{
    struct file *file = find_file(filename);
    if (file) {
        return push_filedesc(file, flags);
    } else if (flags & UFS_CREATE) {
        file = add_file(filename);
        return push_filedesc(file, flags);
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
	    return -1;
    }
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
    if (0 <= fd && fd < file_descriptor_count && file_descriptors[fd]) {
        struct filedesc *filedesc = file_descriptors[fd];
        if (!(filedesc->flag & (UFS_WRITE_ONLY | UFS_READ_WRITE))) {
            ufs_error_code = UFS_ERR_NO_PERMISSION;
            return -1;
        }
        if (MAX_FILE_SIZE < filedesc->totall_offset + size) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
        for (int i = 0; i < size; ++i) {
            if (BLOCK_SIZE <= filedesc->block_offset) {
                if (filedesc->block->next == NULL) {
                    filedesc->block->next = make_block();
                    filedesc->block->next->prev = filedesc->block;
                    filedesc->file->last_block = filedesc->block->next;
                }
                filedesc->block = filedesc->block->next;
                filedesc->block_offset = 0;
            }
            filedesc->block->memory[filedesc->block_offset] = buf[i];
            ++(filedesc->block_offset);
            if (filedesc->block->occupied < filedesc->block_offset) {
                ++(filedesc->block->occupied);
                ++(filedesc->file->size);
            }
            ++(filedesc->totall_offset);
        }
        return size;
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
    if (0 <= fd && fd < file_descriptor_count && file_descriptors[fd]) {
        struct filedesc *filedesc = file_descriptors[fd];
        if (!(filedesc->flag & (UFS_READ_ONLY | UFS_READ_WRITE))) {
            ufs_error_code = UFS_ERR_NO_PERMISSION;
            return -1;
        }
        for (int i = 0; i < size; ++i) {
            if (filedesc->block->occupied <= filedesc->block_offset) {
                if (filedesc->block->next) {
                    filedesc->block = filedesc->block->next;
                    filedesc->block_offset = 0;
                } else {
                    return i;
                }
            }
            buf[i] = filedesc->block->memory[filedesc->block_offset];
            ++(filedesc->block_offset);
        }
        return size;
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }
}

void
delete_file(struct file *file)
{
    struct block *cur_block = file->block_list;
    struct block *next_block = cur_block->next;
    free(cur_block->memory);
    free(cur_block);
    while (next_block) {
        cur_block = next_block;
        next_block = cur_block->next;
        free(cur_block->memory);
        free(cur_block);
    }
    if (file->prev) {
        file->prev->next = file->next;
    } else {
        file_list = file->next;
    }
    if (file->next) {
        file->next->prev = file->prev;
    }
    free(file);
}

int
ufs_close(int fd)
{
    if (0 <= fd && fd < file_descriptor_count && file_descriptors[fd]) {
        --(file_descriptors[fd]->file->refs);
        if (file_descriptors[fd]->file->refs == 0 && file_descriptors[fd]->file->isDeleted) {
            delete_file(file_descriptors[fd]->file);
        }
	    free(file_descriptors[fd]);
        file_descriptors[fd] = NULL;
        return 0;
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }
}

int
ufs_delete(const char *filename)
{
    struct file *file = find_file(filename);
    if (file) {
        if (file->refs) {
            file->isDeleted = 1;
        } else {
            delete_file(file);
        }
        return 0;
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
	    return -1;
    }
}

int
ufs_resize(int fd, size_t new_size)
{
    if (0 <= fd && fd < file_descriptor_count && file_descriptors[fd]) {
        if (new_size > MAX_FILE_SIZE) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
        struct filedesc *filedesc = file_descriptors[fd];
        struct file *file = filedesc->file;
        if (file->size < new_size) {
            new_size -= file->size;
            struct block *block = file->last_block;
            while (new_size > 0) {
                int d = min(new_size, BLOCK_SIZE - block->occupied);
                block->occupied += d;
                file->size += d;
                new_size -= d;
                if (new_size > 0) {
                    block->next = make_block();
                    block->next->prev = block;
                    block = block->next;
                    file->last_block = block;
                }
            }
        } else if (new_size < file->size) {
            new_size = file->size - new_size;
            struct block *block = file->last_block;
            while (new_size > 0) {
                int d = min(new_size, block->occupied);
                block->occupied -= d;
                new_size -= d;
                file->size -= d;
                if (file->size == 0) {
                    break;
                }
                if (block->occupied == 0) {
                    free(block->memory);
                    block = block->prev;
                    free(block->next);
                    block->next = NULL;
                    file->last_block = block;
                }
            }
            if (file->size < filedesc->totall_offset) {
                filedesc->totall_offset = file->size;
                filedesc->block = file->last_block;
                filedesc->block_offset = file->last_block->occupied;
            }
        }
        return 0;
    } else {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }
}
