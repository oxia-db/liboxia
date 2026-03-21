#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../liboxia_ffi.h"

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s (line %d): %s\n", __func__, __LINE__, msg); \
        return 1; \
    } \
} while(0)

#define ASSERT_EQ_INT(a, b, msg) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "FAIL: %s (line %d): %s (expected %d, got %d)\n", \
                __func__, __LINE__, msg, (int)(b), (int)(a)); \
        return 1; \
    } \
} while(0)

static OxiaClient* client = NULL;

static int setup(const char* address) {
    COxiaClientOptions options;
    options.service_address = address;
    options.namespace_ = "default";

    COxiaError err = oxia_client_new(options, &client);
    ASSERT_EQ_INT(err, Ok, "oxia_client_new should succeed");
    ASSERT(client != NULL, "client should not be NULL");
    return 0;
}

/* Test: put and get a key */
static int test_put_get(void) {
    const char* key = "c-test/key1";
    const char* value = "hello-from-c";

    /* Put */
    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)value, strlen(value), &put_result);
    ASSERT_EQ_INT(err, Ok, "put should succeed");
    ASSERT(put_result != NULL, "put_result should not be NULL");
    ASSERT(put_result->version_id >= 0, "version_id should be non-negative");
    ASSERT(strcmp(put_result->key, key) == 0, "put result key should match");
    oxia_put_result_free(put_result);

    /* Get */
    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    ASSERT_EQ_INT(err, Ok, "get should succeed");
    ASSERT(get_result != NULL, "get_result should not be NULL");
    ASSERT(strcmp(get_result->key, key) == 0, "get result key should match");
    ASSERT(get_result->value_len == strlen(value), "value length should match");
    ASSERT(memcmp(get_result->value, value, get_result->value_len) == 0,
           "value content should match");
    ASSERT(get_result->version_id >= 0, "version_id should be non-negative");
    oxia_get_result_free(get_result);

    printf("  PASS: test_put_get\n");
    return 0;
}

/* Test: get non-existent key returns KeyNotFound */
static int test_get_not_found(void) {
    COxiaGetResult* get_result = NULL;
    COxiaError err = oxia_client_get(client, "c-test/nonexistent", &get_result);
    ASSERT_EQ_INT(err, KeyNotFound, "get non-existent key should return KeyNotFound");
    ASSERT(get_result == NULL, "get_result should be NULL on error");

    printf("  PASS: test_get_not_found\n");
    return 0;
}

/* Test: put, delete, then get should return KeyNotFound */
static int test_delete(void) {
    const char* key = "c-test/to-delete";
    const char* value = "deleteme";

    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)value, strlen(value), &put_result);
    ASSERT_EQ_INT(err, Ok, "put should succeed");
    oxia_put_result_free(put_result);

    err = oxia_client_delete(client, key);
    ASSERT_EQ_INT(err, Ok, "delete should succeed");

    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    ASSERT_EQ_INT(err, KeyNotFound, "get after delete should return KeyNotFound");

    printf("  PASS: test_delete\n");
    return 0;
}

/* Test: list keys in range */
static int test_list(void) {
    const char* keys[] = {"c-test/list/a", "c-test/list/b", "c-test/list/c"};
    const char* values[] = {"val-a", "val-b", "val-c"};

    for (int i = 0; i < 3; i++) {
        COxiaPutResult* put_result = NULL;
        COxiaError err = oxia_client_put(client, keys[i],
            (const uint8_t*)values[i], strlen(values[i]), &put_result);
        ASSERT_EQ_INT(err, Ok, "put should succeed");
        oxia_put_result_free(put_result);
    }

    COxiaListResult* list_result = NULL;
    COxiaError err = oxia_client_list(client, "c-test/list/", "c-test/list/~", &list_result);
    ASSERT_EQ_INT(err, Ok, "list should succeed");
    ASSERT(list_result != NULL, "list_result should not be NULL");
    ASSERT(list_result->keys_len == 3, "should have 3 keys");

    /* Keys should be sorted */
    for (int i = 0; i < 3; i++) {
        ASSERT(strcmp(list_result->keys[i], keys[i]) == 0, "key should match");
    }
    oxia_list_result_free(list_result);

    printf("  PASS: test_list\n");
    return 0;
}

/* Test: delete_range removes all keys in range */
static int test_delete_range(void) {
    const char* keys[] = {"c-test/range/x", "c-test/range/y", "c-test/range/z"};
    const char* values[] = {"vx", "vy", "vz"};

    for (int i = 0; i < 3; i++) {
        COxiaPutResult* put_result = NULL;
        COxiaError err = oxia_client_put(client, keys[i],
            (const uint8_t*)values[i], strlen(values[i]), &put_result);
        ASSERT_EQ_INT(err, Ok, "put should succeed");
        oxia_put_result_free(put_result);
    }

    COxiaError err = oxia_client_delete_range(client, "c-test/range/", "c-test/range/~");
    ASSERT_EQ_INT(err, Ok, "delete_range should succeed");

    /* Verify all keys are gone */
    COxiaListResult* list_result = NULL;
    err = oxia_client_list(client, "c-test/range/", "c-test/range/~", &list_result);
    ASSERT_EQ_INT(err, Ok, "list should succeed");
    ASSERT(list_result != NULL, "list_result should not be NULL");
    ASSERT(list_result->keys_len == 0, "should have 0 keys after delete_range");
    oxia_list_result_free(list_result);

    printf("  PASS: test_delete_range\n");
    return 0;
}

/* Test: overwrite a key and verify version changes */
static int test_put_overwrite(void) {
    const char* key = "c-test/overwrite";

    COxiaPutResult* r1 = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)"v1", 2, &r1);
    ASSERT_EQ_INT(err, Ok, "first put should succeed");
    int64_t v1 = r1->version_id;
    oxia_put_result_free(r1);

    COxiaPutResult* r2 = NULL;
    err = oxia_client_put(client, key,
        (const uint8_t*)"v2", 2, &r2);
    ASSERT_EQ_INT(err, Ok, "second put should succeed");
    ASSERT(r2->version_id > v1, "version should increase on overwrite");
    oxia_put_result_free(r2);

    /* Verify latest value */
    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    ASSERT_EQ_INT(err, Ok, "get should succeed");
    ASSERT(get_result->value_len == 2, "value length should be 2");
    ASSERT(memcmp(get_result->value, "v2", 2) == 0, "value should be v2");
    oxia_get_result_free(get_result);

    printf("  PASS: test_put_overwrite\n");
    return 0;
}

/* Test: binary data (not just strings) */
static int test_binary_value(void) {
    const char* key = "c-test/binary";
    uint8_t binary_data[] = {0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F};

    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        binary_data, sizeof(binary_data), &put_result);
    ASSERT_EQ_INT(err, Ok, "put binary should succeed");
    oxia_put_result_free(put_result);

    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    ASSERT_EQ_INT(err, Ok, "get binary should succeed");
    ASSERT(get_result->value_len == sizeof(binary_data), "binary value length should match");
    ASSERT(memcmp(get_result->value, binary_data, sizeof(binary_data)) == 0,
           "binary value content should match");
    oxia_get_result_free(get_result);

    printf("  PASS: test_binary_value\n");
    return 0;
}

int main(int argc, char* argv[]) {
    const char* address = getenv("OXIA_ADDRESS");
    if (!address || strlen(address) == 0) {
        fprintf(stderr, "OXIA_ADDRESS environment variable is required\n");
        return 1;
    }

    printf("Running C FFI tests against %s\n", address);

    if (setup(address) != 0) return 1;

    int failures = 0;
    failures += test_put_get();
    failures += test_get_not_found();
    failures += test_delete();
    failures += test_list();
    failures += test_delete_range();
    failures += test_put_overwrite();
    failures += test_binary_value();

    /* Cleanup: delete all test keys */
    oxia_client_delete_range(client, "c-test/", "c-test/~");

    COxiaError err = oxia_client_shutdown(client);
    if (err != Ok) {
        fprintf(stderr, "WARNING: shutdown returned error %d\n", err);
    }

    printf("\n%s: %d test(s) failed\n", failures == 0 ? "ALL PASSED" : "FAILED", failures);
    return failures;
}
