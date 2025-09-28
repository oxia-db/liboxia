#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../liboxia_ffi.h" // Assumes liboxia_ffi.h is in the parent directory

int main() {
    printf("Initializing Oxia client...\n");

    // 1. Prepare client options
    COxiaClientOptions options;
    options.service_address = "http://127.0.0.1:6648";
    options.namespace_ = "default";

    // 2. Create the client
    OxiaClient* client = NULL;
    COxiaError err_new = oxia_client_new(options, &client);
    if (err_new != Ok) {
        printf("Failed to create client. Error code: %d\n", err_new);
        return 1;
    }
    printf("Client created successfully.\n");

    const char* key = "my_key";
    const char* value = "hello_oxia";
    COxiaPutResult* put_result = NULL;

    // 3. Perform the Put operation
    COxiaError err_put = oxia_client_put(client, key, (const uint8_t*)value, strlen(value), &put_result);
    if (err_put != Ok) {
        printf("Put failed. Error code: %d\n", err_put);
    } else {
        printf("Put successful. Key: %s, Version ID: %lld\n", put_result->key, put_result->version_id);
    }

    // Free the memory for the Put result
    if (put_result != NULL) {
        oxia_put_result_free(put_result);
    }

    // 4. Perform the Get operation
    COxiaGetResult* get_result = NULL;
    COxiaError err_get = oxia_client_get(client, key, &get_result);
    if (err_get != Ok) {
        printf("Get failed. Error code: %d\n", err_get);
    } else {
        printf("Get successful. Key: %s, Value: %s, Version ID: %lld\n",
               get_result->key, (const char*)get_result->value, get_result->version_id);
    }

    // Free the memory for the Get result
    if (get_result != NULL) {
        oxia_get_result_free(get_result);
    }

    // 5. Shutdown the client
    COxiaError err_shutdown = oxia_client_shutdown(client);
    if (err_shutdown != Ok) {
        printf("Client shutdown failed. Error code: %d\n", err_shutdown);
    } else {
        printf("Client shutdown successfully.\n");
    }

    return 0;
}