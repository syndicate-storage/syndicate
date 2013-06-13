/* 
 * File:   JSyndicateFSContext.h
 * Author: iychoi
 *
 * Created on June 6, 2013, 2:29 AM
 */

#ifndef JSYNDICATEFSCONTEXT_H
#define	JSYNDICATEFSCONTEXT_H

#include "syndicate.h"

#ifdef	__cplusplus
extern "C" {
#endif

#define SYNDICATEFS_DATA (jsyndicatefs_get_context()->syndicate_state_data)
    
struct JSyndicateFS_Context {
    struct syndicate_state*     syndicate_state_data;
    struct md_HTTP              syndicate_http;
};

JSyndicateFS_Context* jsyndicatefs_get_context();

#ifdef	__cplusplus
}
#endif

#endif	/* JSYNDICATEFSCONTEXT_H */

