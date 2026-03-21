#ifndef LIBOXIA_FFI_H
#define LIBOXIA_FFI_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

enum COxiaError {
  Ok = 0,
  TransportError = 1,
  GrpcStatus = 2,
  UnexpectedStatus = 3,
  ShardLeaderNotFound = 4,
  KeyLeaderNotFound = 5,
  KeyNotFound = 6,
  UnexpectedVersionId = 7,
  SessionDoesNotExist = 8,
  InternalRetryable = 9,
  Cancelled = 10,
  IllegalArgument = 11,
  RequestTimeout = 12,
};
typedef int32_t COxiaError;

typedef struct OxiaClient OxiaClient;

typedef struct COxiaClientOptions {
  const char *service_address;
  const char *namespace_;
} COxiaClientOptions;

typedef struct COxiaPutResult {
  char *key;
  int64_t version_id;
} COxiaPutResult;

typedef struct COxiaGetResult {
  char *key;
  uint8_t *value;
  uintptr_t value_len;
  int64_t version_id;
} COxiaGetResult;

typedef struct COxiaListResult {
  char **keys;
  uintptr_t keys_len;
} COxiaListResult;

typedef struct COxiaRangeScanRecord {
  char *key;
  uint8_t *value;
  uintptr_t value_len;
  int64_t version_id;
} COxiaRangeScanRecord;

typedef struct COxiaRangeScanResult {
  struct COxiaRangeScanRecord *records;
  uintptr_t records_len;
} COxiaRangeScanResult;

COxiaError oxia_client_new(struct COxiaClientOptions options, struct OxiaClient **client_ptr);

void oxia_client_free(struct OxiaClient *client);

COxiaError oxia_client_put(const struct OxiaClient *client,
                           const char *key,
                           const uint8_t *value,
                           uintptr_t value_len,
                           struct COxiaPutResult **result_ptr);

COxiaError oxia_client_get(const struct OxiaClient *client,
                           const char *key,
                           struct COxiaGetResult **result_ptr);

COxiaError oxia_client_shutdown(struct OxiaClient *client);

void oxia_put_result_free(struct COxiaPutResult *result);

COxiaError oxia_client_delete(const struct OxiaClient *client, const char *key);

COxiaError oxia_client_list(const struct OxiaClient *client,
                            const char *min_key_inclusive,
                            const char *max_key_exclusive,
                            struct COxiaListResult **result_ptr);

void oxia_list_result_free(struct COxiaListResult *result);

COxiaError oxia_client_delete_range(const struct OxiaClient *client,
                                    const char *min_key_inclusive,
                                    const char *max_key_exclusive);

COxiaError oxia_client_range_scan(const struct OxiaClient *client,
                                  const char *min_key_inclusive,
                                  const char *max_key_exclusive,
                                  struct COxiaRangeScanResult **result_ptr);

void oxia_range_scan_result_free(struct COxiaRangeScanResult *result);

void oxia_get_result_free(struct COxiaGetResult *result);

#endif  /* LIBOXIA_FFI_H */
