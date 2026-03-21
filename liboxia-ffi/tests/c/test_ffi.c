#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../liboxia_ffi.h"
#include "vendor/unity/unity.h"

static OxiaClient* client = NULL;

void setUp(void) {
    /* Called before each test — nothing to do, client is shared */
}

void tearDown(void) {
    /* Called after each test — nothing to do */
}

/* ============================================================
 * Test: put and get a key
 * ============================================================ */
void test_put_get(void) {
    const char* key = "c-test/key1";
    const char* value = "hello-from-c";

    /* Put */
    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)value, strlen(value), &put_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_NOT_NULL(put_result);
    TEST_ASSERT_TRUE(put_result->version_id >= 0);
    TEST_ASSERT_EQUAL_STRING(key, put_result->key);
    oxia_put_result_free(put_result);

    /* Get */
    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_NOT_NULL(get_result);
    TEST_ASSERT_EQUAL_STRING(key, get_result->key);
    TEST_ASSERT_EQUAL_UINT(strlen(value), get_result->value_len);
    TEST_ASSERT_EQUAL_MEMORY(value, get_result->value, get_result->value_len);
    TEST_ASSERT_TRUE(get_result->version_id >= 0);
    oxia_get_result_free(get_result);
}

/* ============================================================
 * Test: get non-existent key returns KeyNotFound
 * ============================================================ */
void test_get_not_found(void) {
    COxiaGetResult* get_result = NULL;
    COxiaError err = oxia_client_get(client, "c-test/nonexistent", &get_result);
    TEST_ASSERT_EQUAL_INT(KeyNotFound, err);
    TEST_ASSERT_NULL(get_result);
}

/* ============================================================
 * Test: put, delete, then get should return KeyNotFound
 * ============================================================ */
void test_delete(void) {
    const char* key = "c-test/to-delete";
    const char* value = "deleteme";

    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)value, strlen(value), &put_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    oxia_put_result_free(put_result);

    err = oxia_client_delete(client, key);
    TEST_ASSERT_EQUAL_INT(Ok, err);

    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    TEST_ASSERT_EQUAL_INT(KeyNotFound, err);
}

/* ============================================================
 * Test: list keys in range
 * ============================================================ */
void test_list(void) {
    const char* keys[] = {"c-test/list/a", "c-test/list/b", "c-test/list/c"};
    const char* values[] = {"val-a", "val-b", "val-c"};

    for (int i = 0; i < 3; i++) {
        COxiaPutResult* put_result = NULL;
        COxiaError err = oxia_client_put(client, keys[i],
            (const uint8_t*)values[i], strlen(values[i]), &put_result);
        TEST_ASSERT_EQUAL_INT(Ok, err);
        oxia_put_result_free(put_result);
    }

    COxiaListResult* list_result = NULL;
    COxiaError err = oxia_client_list(client, "c-test/list/", "c-test/list/~", &list_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_NOT_NULL(list_result);
    TEST_ASSERT_EQUAL_UINT(3, list_result->keys_len);

    /* Keys should be sorted */
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL_STRING(keys[i], list_result->keys[i]);
    }
    oxia_list_result_free(list_result);
}

/* ============================================================
 * Test: delete_range removes all keys in range
 * ============================================================ */
void test_delete_range(void) {
    const char* keys[] = {"c-test/range/x", "c-test/range/y", "c-test/range/z"};
    const char* values[] = {"vx", "vy", "vz"};

    for (int i = 0; i < 3; i++) {
        COxiaPutResult* put_result = NULL;
        COxiaError err = oxia_client_put(client, keys[i],
            (const uint8_t*)values[i], strlen(values[i]), &put_result);
        TEST_ASSERT_EQUAL_INT(Ok, err);
        oxia_put_result_free(put_result);
    }

    COxiaError err = oxia_client_delete_range(client, "c-test/range/", "c-test/range/~");
    TEST_ASSERT_EQUAL_INT(Ok, err);

    /* Verify all keys are gone */
    COxiaListResult* list_result = NULL;
    err = oxia_client_list(client, "c-test/range/", "c-test/range/~", &list_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_NOT_NULL(list_result);
    TEST_ASSERT_EQUAL_UINT(0, list_result->keys_len);
    oxia_list_result_free(list_result);
}

/* ============================================================
 * Test: overwrite a key and verify version changes
 * ============================================================ */
void test_put_overwrite(void) {
    const char* key = "c-test/overwrite";

    COxiaPutResult* r1 = NULL;
    COxiaError err = oxia_client_put(client, key,
        (const uint8_t*)"v1", 2, &r1);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    int64_t v1 = r1->version_id;
    oxia_put_result_free(r1);

    COxiaPutResult* r2 = NULL;
    err = oxia_client_put(client, key,
        (const uint8_t*)"v2", 2, &r2);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_TRUE(r2->version_id > v1);
    oxia_put_result_free(r2);

    /* Verify latest value */
    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_EQUAL_UINT(2, get_result->value_len);
    TEST_ASSERT_EQUAL_MEMORY("v2", get_result->value, 2);
    oxia_get_result_free(get_result);
}

/* ============================================================
 * Test: binary data (not just strings)
 * ============================================================ */
void test_binary_value(void) {
    const char* key = "c-test/binary";
    uint8_t binary_data[] = {0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F};

    COxiaPutResult* put_result = NULL;
    COxiaError err = oxia_client_put(client, key,
        binary_data, sizeof(binary_data), &put_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    oxia_put_result_free(put_result);

    COxiaGetResult* get_result = NULL;
    err = oxia_client_get(client, key, &get_result);
    TEST_ASSERT_EQUAL_INT(Ok, err);
    TEST_ASSERT_EQUAL_UINT(sizeof(binary_data), get_result->value_len);
    TEST_ASSERT_EQUAL_MEMORY(binary_data, get_result->value, sizeof(binary_data));
    oxia_get_result_free(get_result);
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;

    const char* address = getenv("OXIA_ADDRESS");
    if (!address || strlen(address) == 0) {
        fprintf(stderr, "OXIA_ADDRESS environment variable is required\n");
        return 1;
    }

    /* Setup: create client */
    COxiaClientOptions options;
    options.service_address = address;
    options.namespace_ = "default";

    COxiaError err = oxia_client_new(options, &client);
    if (err != Ok) {
        fprintf(stderr, "Failed to create client. Error code: %d\n", err);
        return 1;
    }

    UNITY_BEGIN();
    RUN_TEST(test_put_get);
    RUN_TEST(test_get_not_found);
    RUN_TEST(test_delete);
    RUN_TEST(test_list);
    RUN_TEST(test_delete_range);
    RUN_TEST(test_put_overwrite);
    RUN_TEST(test_binary_value);
    int result = UNITY_END();

    /* Cleanup */
    oxia_client_delete_range(client, "c-test/", "c-test/~");
    oxia_client_shutdown(client);

    return result;
}
