#include "JSyndicateFSContext.h"

/*
 * Global Variable
 */
static JSyndicateFS_Context native_context;

/*
 * Public Functions
 */
JSyndicateFS_Context* jsyndicatefs_get_context() {
    return &native_context;
}
