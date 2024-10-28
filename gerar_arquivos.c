#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define MAX_EVENT_TIME_LEN 64
#define MAX_EVENT_TYPE_LEN 32
#define MAX_USER_SESSION_LEN 64
#define MAX_CATEGORY_CODE_LEN 64
#define MAX_BRAND_LEN 32
#define CHUNK_SIZE 131072
#define FILE_ABSOLUTE_PATH "/Users/gabriel/Desktop/projects/c/dataset/"

typedef struct
{
	long long head_index;
	int sequential_sorted;
}

Header;

typedef struct
{
	char event_time[MAX_EVENT_TIME_LEN];
	char event_type[MAX_EVENT_TYPE_LEN];
	long long product_id;
	long long user_id;
	char user_session[MAX_USER_SESSION_LEN];
	long long seq_key;
}

AccessRecord;

typedef struct
{
	long long product_id;
	long long category_id;
	char category_code[MAX_CATEGORY_CODE_LEN];
	char brand[MAX_BRAND_LEN];
	float price;
	int ativo;
	long long seq_key;
	long long elo;
}

ProductRecord;

void create_accesses_file(const char *input_filename, const char *output_filename);
void external_sort_products(const char *input_filename, const char *output_filename);
void merge_files(const char *output_filename, char **temp_files, int num_temp_files, size_t record_size, int(*compare)(const void *, const void *));
int compare_access_records(const void *a, const void *b);
int compare_product_records(const void *a, const void *b);
void pad_string(char *str, int size);
void quicksort(void *arr, int left, int right, size_t size, int(*compare)(const void *, const void *));
int partition(void *arr, int left, int right, size_t size, int(*compare)(const void *, const void *));
void swap_records(void *a, void *b, size_t size);

int main()
{
	const char *input_filename = FILE_ABSOLUTE_PATH "dados_oct.csv";
	create_accesses_file(input_filename, FILE_ABSOLUTE_PATH "accesses.bin");
	// external_sort_products(input_filename, FILE_ABSOLUTE_PATH "products.bin");
	return 0;
}

void create_accesses_file(const char *input_filename, const char *output_filename)
{
	FILE *fp = fopen(input_filename, "r");
	FILE *output_fp = fopen(output_filename, "wb");
	size_t access_capacity = CHUNK_SIZE;
	AccessRecord *access_records = malloc(access_capacity* sizeof(AccessRecord));
	char line[1024];
	long long seq_counter = 1;

	if (fgets(line, sizeof(line), fp))
	{
		if (strstr(line, "event_time") == NULL)
		{
			fseek(fp, 0, SEEK_SET);
		}
	}

	while (1)
	{
		size_t access_count = 0;

		while (access_count < access_capacity && fgets(line, sizeof(line), fp))
		{
			char *p = line;
			int field = 0;
			char *token;

			char event_time[MAX_EVENT_TIME_LEN];
			char event_type[MAX_EVENT_TYPE_LEN];
			long long product_id = 0;
			long long user_id = 0;
			char user_session[MAX_USER_SESSION_LEN];

			memset(event_time, 0, sizeof(event_time));
			memset(event_type, 0, sizeof(event_type));
			memset(user_session, 0, sizeof(user_session));

			while (field <= 8)
			{
				token = p;
				while (*p && *p != ',') p++;
				if (*p == ',')
				{ 		*p = '\0';
					p++;
				}
				else if (*p == '\n' || *p == '\0')
				{ 		*p = '\0';
					p = NULL;
				}

				switch (field)
				{
					case 0:
						strncpy(event_time, token, MAX_EVENT_TIME_LEN - 1);
						pad_string(event_time, MAX_EVENT_TIME_LEN - 1);
						break;
					case 1:
						strncpy(event_type, token, MAX_EVENT_TYPE_LEN - 1);
						pad_string(event_type, MAX_EVENT_TYPE_LEN - 1);
						break;
					case 2:
						if (*token != '\0')
						{
							product_id = atoll(token);
						}

						break;
					case 7:
						if (*token != '\0')
						{
							user_id = atoll(token);
						}

						break;
					case 8:
						strncpy(user_session, token, MAX_USER_SESSION_LEN - 1);
						user_session[strcspn(user_session, "\n")] = '\0';
						pad_string(user_session, MAX_USER_SESSION_LEN - 1);
						break;
					default:
						break;
				}

				if (p == NULL)
				{
					break;
				}

				field++;
			}

			AccessRecord *access_rec = &access_records[access_count++];
			strcpy(access_rec->event_time, event_time);
			strcpy(access_rec->event_type, event_type);
			access_rec->product_id = product_id;
			access_rec->user_id = user_id;
			strcpy(access_rec->user_session, user_session);
			access_rec->seq_key = seq_counter++;
		}

		if (access_count == 0)
		{
			break;
		}

		fwrite(access_records, sizeof(AccessRecord), access_count, output_fp);
	}

	free(access_records);
	fclose(fp);
	fclose(output_fp);
}

void external_sort_products(const char *input_filename, const char *output_filename)
{
	FILE *fp = fopen(input_filename, "r");
	char **temp_files = NULL;
	int temp_file_count = 0;
	size_t product_capacity = CHUNK_SIZE;
	ProductRecord *product_records = malloc(product_capacity* sizeof(ProductRecord));
	char line[1024];

	if (fgets(line, sizeof(line), fp))
	{
		if (strstr(line, "event_time") == NULL)
		{
			fseek(fp, 0, SEEK_SET);
		}
	}

	while (1)
	{
		size_t product_count = 0;

		while (product_count < product_capacity && fgets(line, sizeof(line), fp))
		{
			char *p = line;
			char *token;
			int field = 0;

			long long product_id = 0;
			long long category_id = 0;
			char category_code[MAX_CATEGORY_CODE_LEN];
			char brand[MAX_BRAND_LEN];
			float price = 0.0;

			memset(category_code, 0, sizeof(category_code));
			memset(brand, 0, sizeof(brand));

			while (field <= 6)
			{
				token = p;
				while (*p && *p != ',') p++;
				if (*p == ',')
				{ 		*p = '\0';
					p++;
				}
				else if (*p == '\n' || *p == '\0')
				{ 		*p = '\0';
					p = NULL;
				}

				switch (field)
				{
					case 2:
						if (*token != '\0')
						{
							product_id = atoll(token);
						}

						break;
					case 3:
						if (*token != '\0')
						{
							category_id = atoll(token);
						}

						break;
					case 4:
						strncpy(category_code, token, MAX_CATEGORY_CODE_LEN - 1);
						pad_string(category_code, MAX_CATEGORY_CODE_LEN - 1);
						break;
					case 5:
						strncpy(brand, token, MAX_BRAND_LEN - 1);
						pad_string(brand, MAX_BRAND_LEN - 1);
						break;
					case 6:
						if (*token != '\0')
						{
							price = atof(token);
						}

						break;
					default:
						break;
				}

				field++;
				if (p == NULL)
				{
					break;
				}
			}

			ProductRecord *product_rec = &product_records[product_count++];
			product_rec->product_id = product_id;
			product_rec->category_id = category_id;
			strcpy(product_rec->category_code, category_code);
			strcpy(product_rec->brand, brand);
			product_rec->price = price;
			product_rec->seq_key = 0;
			product_rec->ativo = 1;
			product_rec->elo = 0;
		}

		if (product_count == 0)
		{
			break;
		}

		quicksort(product_records, 0, product_count - 1, sizeof(ProductRecord), compare_product_records);

		char temp_filename[512];
		sprintf(temp_filename, FILE_ABSOLUTE_PATH "product_temp_%d.bin", temp_file_count++);
		FILE *temp_fp = fopen(temp_filename, "wb");
		fwrite(product_records, sizeof(ProductRecord), product_count, temp_fp);
		fclose(temp_fp);

		char *temp_filename_dup = strdup(temp_filename);
		char **new_temp_files = realloc(temp_files, temp_file_count* sizeof(char*));
		temp_files = new_temp_files;
		temp_files[temp_file_count - 1] = temp_filename_dup;
	}

	free(product_records);
	fclose(fp);

	merge_files(output_filename, temp_files, temp_file_count, sizeof(ProductRecord), compare_product_records);

	for (int i = 0; i < temp_file_count; i++)
	{
		remove(temp_files[i]);
		free(temp_files[i]);
	}

	free(temp_files);
}

void merge_files(const char *output_filename, char **temp_files, int num_temp_files, size_t record_size, int(*compare)(const void *, const void *))
{
	FILE **fps = malloc(num_temp_files* sizeof(FILE*));
	for (int i = 0; i < num_temp_files; i++)
	{
		fps[i] = fopen(temp_files[i], "rb");
	}

	ProductRecord *record = malloc(sizeof(ProductRecord));
	void **buffers = malloc(num_temp_files* sizeof(void*));
	int *active = malloc(num_temp_files* sizeof(int));
	for (int i = 0; i < num_temp_files; i++)
	{
		buffers[i] = malloc(record_size);
		if (fread(buffers[i], record_size, 1, fps[i]) == 1)
		{
			active[i] = 1;
		}
		else
		{
			active[i] = 0;
		}
	}

	FILE *output_fp = fopen(output_filename, "wb+");
	Header header;
	header.head_index = 1;
	header.sequential_sorted = 1;
	fwrite(&header, sizeof(Header), 1, output_fp);
	int first_record = 1;
	long long seq_counter = 1;
	long last_written_pos = -1;

	void *last_written_record = malloc(record_size);

	while (1)
	{
		int min_index = -1;
		for (int i = 0; i < num_temp_files; i++)
		{
			if (active[i])
			{
				if (min_index == -1)
				{
					min_index = i;
				}
				else
				{
					if (compare(buffers[i], buffers[min_index]) < 0)
					{
						min_index = i;
					}
				}
			}
		}

		if (min_index == -1)
		{
			break;
		}

		memcpy(record, buffers[min_index], sizeof(ProductRecord));

		if (first_record || compare(buffers[min_index], last_written_record) != 0)
		{
			record->elo = 0;
			record->seq_key = seq_counter;
			last_written_pos = ftell(output_fp);
			fwrite(record, sizeof(ProductRecord), 1, output_fp);
			memcpy(last_written_record, record, sizeof(ProductRecord));
			first_record = 0;
			seq_counter++;
		}

		if (fread(buffers[min_index], record_size, 1, fps[min_index]) == 1)
		{
			active[min_index] = 1;
		}
		else
		{
			active[min_index] = 0;
		}
	}

	if (last_written_pos != -1)
	{
		fseek(output_fp, last_written_pos, SEEK_SET);
		fread(last_written_record, record_size, 1, output_fp);
		ProductRecord *last_record = (ProductRecord*) last_written_record;
		last_record->elo = -1;
		fseek(output_fp, last_written_pos, SEEK_SET);
		fwrite(last_record, sizeof(ProductRecord), 1, output_fp);
	}

	fclose(output_fp);
	free(last_written_record);
	free(record);
	for (int i = 0; i < num_temp_files; i++)
	{
		fclose(fps[i]);
		free(buffers[i]);
	}

	free(fps);
	free(buffers);
	free(active);
}

int compare_product_records(const void *a, const void *b)
{
	const ProductRecord *recA = (const ProductRecord *) a;
	const ProductRecord *recB = (const ProductRecord *) b;

	if (recA->product_id < recB->product_id)
		return -1;
	else if (recA->product_id > recB->product_id)
		return 1;
	else
		return 0;
}

void pad_string(char *str, int size)
{
	int len = strlen(str);
	for (int i = len; i < size; i++)
	{
		str[i] = ' ';
	}

	str[size] = '\0';
}

void quicksort(void *arr, int left, int right, size_t size, int(*compare)(const void *, const void *))
{
	if (left < right)
	{
		int pivot_index = partition(arr, left, right, size, compare);
		quicksort(arr, left, pivot_index - 1, size, compare);
		quicksort(arr, pivot_index + 1, right, size, compare);
	}
}

int partition(void *arr, int left, int right, size_t size, int(*compare)(const void *, const void *))
{
	char *array = (char*) arr;
	void *pivot = array + right * size;
	int i = left - 1;

	for (int j = left; j < right; j++)
	{
		if (compare(array + j *size, pivot) <= 0)
		{
			i++;
			swap_records(array + i *size, array + j *size, size);
		}
	}

	swap_records(array + (i + 1) *size, array + right *size, size);
	return i + 1;
}

void swap_records(void *a, void *b, size_t size)
{
	char temp[sizeof(ProductRecord) > sizeof(AccessRecord) ? sizeof(ProductRecord) : sizeof(AccessRecord)];
	memcpy(temp, a, size);
	memcpy(a, b, size);
	memcpy(b, temp, size);
}