# MS build parameters 

MS_APP_ADMIN_EMAIL				?= jcnelson@cs.princeton.edu
MS_APP_ADMIN_PUBLIC_KEY			?= $(BUILD_MS)/admin.pub
MS_APP_ADMIN_PRIVATE_KEY		?= $(BUILD_MS)/admin.pem

MS_APP_NAME					?= syndicate-metadata
MS_APP_PUBLIC_KEY			?= $(BUILD_MS)/syndicate.pub
MS_APP_PRIVATE_KEY		?= $(BUILD_MS)/syndicate.pem

MS_DEVEL						?= true

$(MS_APP_ADMIN_PRIVATE_KEY):
	openssl genrsa 4096 > "$@"

$(MS_APP_ADMIN_PUBLIC_KEY): $(MS_APP_ADMIN_PRIVATE_KEY)
	openssl rsa -in "$<" -pubout > "$@"

$(MS_APP_PRIVATE_KEY):
	openssl genrsa 4096 > "$@"

$(MS_APP_PUBLIC_KEY): $(MS_APP_PRIVATE_KEY)
	openssl rsa -in "$<" -pubout > "$@"

