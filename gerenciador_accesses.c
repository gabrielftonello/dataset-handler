#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define MAX_EVENT_TIME_LEN 64
#define MAX_EVENT_TYPE_LEN 32
#define MAX_USER_SESSION_LEN 64
#define FILE_ABSOLUTE_PATH "/Users/gabriel/Desktop/projects/c/dataset/"
#define ORIGINAL_FILE_NAME FILE_ABSOLUTE_PATH "accesses.bin"
#define INDEX_FILE_NAME FILE_ABSOLUTE_PATH "accesses.idx"
#define RECORDS_PER_PAGE 16
#define INDEX_INTERVAL 1024

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
	long long seq_key;
}

IndexRecord;

int binary_search_seq_key(long long target_seq_key, AccessRecord *result)
{
	FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");

	fseek(fp, 0, SEEK_END);
	long long file_size = ftell(fp);
	long long num_records = file_size / sizeof(AccessRecord);
	long long left = 0;
	long long right = num_records - 1;
	long long mid;
	AccessRecord mid_record;

	while (left <= right)
	{
		mid = left + (right - left) / 2;
		fseek(fp, mid* sizeof(AccessRecord), SEEK_SET);
		fread(&mid_record, sizeof(AccessRecord), 1, fp);
		if (mid_record.seq_key == target_seq_key)
		{*result = mid_record;
			fclose(fp);
			return mid;
		}
		else if (mid_record.seq_key < target_seq_key)
		{
			left = mid + 1;
		}
		else
		{
			if (mid == 0) break;
			right = mid - 1;
		}
	}

	fclose(fp);
	return -1;
}

void search_and_display_access(long long target_seq_key)
{
	AccessRecord found_record;
	long long found_index = binary_search_seq_key(target_seq_key, &found_record);

	if (found_index != -1)
	{
		printf("\nAcesso encontrado no indice %lld:\n", found_index + 1);
		printf("  Seq Key: %lld\n", found_record.seq_key);
		printf("  Product ID: %lld\n", found_record.product_id);
		printf("  User ID: %lld\n", found_record.user_id);
		printf("  User Session: %s\n", found_record.user_session);
		printf("  Event time: %s\n", found_record.event_time);
		printf("  Event type: %s\n", found_record.event_type);
	}
	else
	{
		printf("\nAcesso com seq_key %lld nao encontrado.\n", target_seq_key);
	}
}

void print_all_records_sequential(long long pag)
{
	FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");

	fseek(fp, 0, SEEK_END);
	long long file_size = ftell(fp);
	long long num_records = file_size / sizeof(AccessRecord);

	if (pag < 1 || (pag - 1) *RECORDS_PER_PAGE >= num_records)
	{
		printf("\nEntrada invalida.\n");
		fclose(fp);
		return;
	}

	long long start_record = (pag - 1) *RECORDS_PER_PAGE;
	fseek(fp, start_record* sizeof(AccessRecord), SEEK_SET);

	printf("\nExibindo registros da pagina %lld:\n", pag);
	AccessRecord record;
	for (long long i = 0; i < RECORDS_PER_PAGE && (start_record + i) < num_records; i++)
	{
		fread(&record, sizeof(AccessRecord), 1, fp);

		printf("\nRegistro %lld:\n", start_record + i + 1);
		printf("  Seq Key: %lld\n", record.seq_key);
		printf("  Product ID: %lld\n", record.product_id);
		printf("  User ID: %lld\n", record.user_id);
		printf("  User Session: %s\n", record.user_session);
		printf("  Event time: %s\n", record.event_time);
		printf("  Event type: %s\n", record.event_type);
	}

	fclose(fp);
}

void build_partial_index_file()
{
	printf("\nConstruindo o indice parcial do arquivo de dados...\n");

	FILE *data_fp = fopen(ORIGINAL_FILE_NAME, "rb");
	FILE *index_fp = fopen(INDEX_FILE_NAME, "wb");

	long long record_count = 0;
	AccessRecord record;

	while (fread(&record, sizeof(AccessRecord), 1, data_fp) == 1)
	{
		if (record_count % INDEX_INTERVAL == 0)
		{
			IndexRecord idx_record;
			idx_record.seq_key = record.seq_key;
			fwrite(&idx_record, sizeof(IndexRecord), 1, index_fp);
		}

		record_count++;
	}

	fclose(data_fp);
	fclose(index_fp);

}

void search_and_display_product_via_index(long long target_seq_key)
{
	AccessRecord access_result;

	FILE *index_fp = fopen(INDEX_FILE_NAME, "rb");

	if (index_fp == NULL)
	{
		build_partial_index_file();
		index_fp = fopen(INDEX_FILE_NAME, "rb");
	}

	fseek(index_fp, 0, SEEK_END);
	long long file_size = ftell(index_fp);
	long long num_records = file_size / sizeof(IndexRecord);
	long long left = 0;
	long long right = num_records - 1;
	long long mid;
	IndexRecord index_record;

	while (left <= right)
	{
		mid = left + (right - left) / 2;
		fseek(index_fp, mid* sizeof(IndexRecord), SEEK_SET);
		fread(&index_record, sizeof(IndexRecord), 1, index_fp);
		if (index_record.seq_key == target_seq_key)
		{
			break;
		}
		else if (index_record.seq_key < target_seq_key)
		{
			if (mid == num_records - 1) break;
			left = mid + 1;
		}
		else
		{
			if (mid == 0) break;
			right = mid - 1;
		}
	}

	FILE *data_fp = fopen(ORIGINAL_FILE_NAME, "rb");

	if (index_record.seq_key == target_seq_key)
	{
		fseek(data_fp, (index_record.seq_key - 1) *sizeof(AccessRecord), SEEK_SET);
		fread(&access_result, sizeof(AccessRecord), 1, data_fp);
	}
	else if (mid == num_records - 1)
	{
		fseek(data_fp, 0, SEEK_END);
		file_size = ftell(data_fp);
		num_records = file_size / sizeof(AccessRecord);
		left = index_record.seq_key - 1;
		right = num_records - 1;
		AccessRecord access_record;
		while (left <= right)
		{
			mid = left + (right - left) / 2;
			fseek(data_fp, mid* sizeof(AccessRecord), SEEK_SET);
			fread(&access_record, sizeof(AccessRecord), 1, data_fp);
			if (access_record.seq_key == target_seq_key)
			{
				access_result = access_record;
				break;
			}
			else if (access_record.seq_key < target_seq_key)
			{
				left = mid + 1;
			}
			else
			{
				if (mid == 0) break;
				right = mid - 1;
			}
		}
	}
	else if (mid != 0 || left != 0)
	{
		IndexRecord record;
		fseek(index_fp, left* sizeof(IndexRecord), SEEK_SET);
		fread(&record, sizeof(IndexRecord), 1, index_fp);
		left = right;
		right = record.seq_key - 1;
		fseek(index_fp, left* sizeof(IndexRecord), SEEK_SET);
		fread(&record, sizeof(IndexRecord), 1, index_fp);
		left = record.seq_key - 1;
		AccessRecord access_record;
		while (left <= right)
		{
			mid = left + (right - left) / 2;
			fseek(data_fp, mid* sizeof(AccessRecord), SEEK_SET);
			fread(&access_record, sizeof(AccessRecord), 1, data_fp);
			if (access_record.seq_key == target_seq_key)
			{
				access_result = access_record;
				break;
			}
			else if (access_record.seq_key < target_seq_key)
			{
				if (mid == num_records - 1) break;
				left = mid + 1;
			}
			else
			{
				if (mid == 0) break;
				right = mid - 1;
			}
		}
	}

	if (access_result.seq_key == target_seq_key)
	{
		printf("\nProduto encontrado no indice %lld:\n", access_result.seq_key);
		printf("  Seq Key: %lld\n", access_result.seq_key);
		printf("  Product ID: %lld\n", access_result.product_id);
		printf("  User ID: %lld\n", access_result.user_id);
		printf("  User Session: %s\n", access_result.user_session);
		printf("  Event time: %s\n", access_result.event_time);
		printf("  Event type: %s\n", access_result.event_type);
	}
	else
	{
		printf("\nProduto com seq_key %lld nao encontrado.\n", target_seq_key);
	}

	fclose(data_fp);
	fclose(index_fp);

}

void create_user_access_file(long long user_id)
{
	char user_file_name[256];
	snprintf(user_file_name, sizeof(user_file_name), FILE_ABSOLUTE_PATH "user_%lld.bin", user_id);

	FILE *orig_fp = fopen(ORIGINAL_FILE_NAME, "rb");
	FILE *user_fp = fopen(user_file_name, "wb");

	AccessRecord record;
	while (fread(&record, sizeof(AccessRecord), 1, orig_fp) == 1)
	{
		if (record.user_id == user_id)
		{
			fwrite(&record, sizeof(AccessRecord), 1, user_fp);
		}
	}

	fclose(orig_fp);
	fclose(user_fp);
}

void display_user_access_records(long long user_id)
{
	char user_file_name[256];
	snprintf(user_file_name, sizeof(user_file_name), FILE_ABSOLUTE_PATH "user_%lld.bin", user_id);

	FILE *user_fp = fopen(user_file_name, "rb");

	AccessRecord records[RECORDS_PER_PAGE];
	int records_read;
	int page = 1;
	char user_input[10];
	getchar();
	do {
		records_read = fread(records, sizeof(AccessRecord), RECORDS_PER_PAGE, user_fp);
		if (records_read == 0)
		{
			printf("\nNao existe ou nao existe mais registros para exibir.\n");
			break;
		}

		printf("\nExibindo pagina %d:\n", page);
		for (int i = 0; i < records_read; i++)
		{
			AccessRecord *record = &records[i];
			printf("\nRegistro %d:\n", (page - 1) *RECORDS_PER_PAGE + i + 1);
			printf("  Seq Key: %lld\n", record->seq_key);
			printf("  Product ID: %lld\n", record->product_id);
			printf("  User ID: %lld\n", record->user_id);
			printf("  User Session: %s\n", record->user_session);
			printf("  Event time: %s\n", record->event_time);
			printf("  Event type: %s", record->event_type);
		}


		printf("\nExibir a proxima pagina? (s/n): ");
		fgets(user_input, sizeof(user_input), stdin);

		size_t len = strlen(user_input);
		if (len > 0 && user_input[len - 1] == '\n')
		{
			user_input[len - 1] = '\0';
		}

		page++;
	} while (strcmp(user_input, "s") == 0 || strcmp(user_input, "S") == 0);

	fclose(user_fp);
	remove(user_file_name);

}

void create_product_access_file(long long product_id)
{
    char product_file_name[256];
    snprintf(product_file_name, sizeof(product_file_name), FILE_ABSOLUTE_PATH "product_%lld.bin", product_id);

    FILE *orig_fp = fopen(ORIGINAL_FILE_NAME, "rb");
    FILE *product_fp = fopen(product_file_name, "wb");

    AccessRecord record;
    while (fread(&record, sizeof(AccessRecord), 1, orig_fp) == 1)
    {
        if (record.product_id == product_id)
        {
            fwrite(&record, sizeof(AccessRecord), 1, product_fp);
        }
    }

    fclose(orig_fp);
    fclose(product_fp);
}

void display_product_access_records(long long product_id)
{
    char product_file_name[256];
    snprintf(product_file_name, sizeof(product_file_name), FILE_ABSOLUTE_PATH "product_%lld.bin", product_id);

    FILE *product_fp = fopen(product_file_name, "rb");

    AccessRecord records[RECORDS_PER_PAGE];
    int records_read;
    int page = 1;
    char user_input[10];
	getchar();

    do {
        records_read = fread(records, sizeof(AccessRecord), RECORDS_PER_PAGE, product_fp);
        if (records_read == 0)
        {
            printf("\nNao existe ou nao existe mais registros para exibir.\n");
            break;
        }

        printf("\nExibindo pagina %d:\n\n", page);
        for (int i = 0; i < records_read; i++)
        {
            AccessRecord *record = &records[i];
            printf("\nRegistro %d:\n", (page - 1) * RECORDS_PER_PAGE + i + 1);
            printf("  Seq Key: %lld\n", record->seq_key);
            printf("  Product ID: %lld\n", record->product_id);
            printf("  User ID: %lld\n", record->user_id);
            printf("  User Session: %s\n", record->user_session);
            printf("  Event time: %s\n", record->event_time);
            printf("  Event type: %s\n", record->event_type);
        }

        printf("Exibir a proxima pagina? (s/n): ");
        fgets(user_input, sizeof(user_input), stdin);

        size_t len = strlen(user_input);
        if (len > 0 && user_input[len - 1] == '\n')
        {
            user_input[len - 1] = '\0';
        }

        page++;
    } while (strcmp(user_input, "s") == 0 || strcmp(user_input, "S") == 0);

    fclose(product_fp);
    remove(product_file_name);

}

int main()
{
	int choice;
	long long target_seq_key, page;

	do {
		printf("\nMenu:\n");
		printf("1. Buscar acesso por seq_key no arquivo de dados\n");
		printf("2. Mostrar registros sequencialmente (por pagina)\n");
		printf("3. Buscar acesso via indice\n");
		printf("4. Buscar acesso por user_id\n");
		printf("5. Buscar acesso por product_id\n");
		printf("6. Sair\n");
		printf("Escolha uma opcao: ");
		scanf("%d", &choice);

		switch (choice)
		{
			case 1:
				printf("Digite o seq_key que deseja buscar: ");
				scanf("%lld", &target_seq_key);
				search_and_display_access(target_seq_key);
				break;
			case 2:
				printf("Digite o numero da pagina: ");
				scanf("%lld", &page);
				print_all_records_sequential(page);
				break;
			case 3:
				printf("Digite o seq_key do produto que deseja buscar: ");
				scanf("%lld", &target_seq_key);
				search_and_display_product_via_index(target_seq_key);
				break;
			case 4: 
			{
				long long user_id;
				printf("\nDigite o user_id: ");
				scanf("%lld", &user_id);
				create_user_access_file(user_id);
				display_user_access_records(user_id);
				break;
			}
			case 5: 
			{
				long long product_id;
				printf("\nDigite o product_id: ");
				scanf("%lld", &product_id);
				create_product_access_file(product_id);
				display_product_access_records(product_id);
				break;
			}
			case 6:
				printf("Saindo...\n");
				break;
			default:
				printf("Opcao invalida. Tente novamente.\n");
				break;
		}
	} while (choice != 6);

	return 0;
}