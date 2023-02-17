ALCHEMY_API_KEY="7lVejDHBMuwziiLwAsmQFpSz8KtSwQTC"
BUCKET_NAME="polygon-karma3labs"
NODE_ENDPOINT="https://nd-361-574-540.p2pify.com/c1145b99186e9e3e4062d2ec09b782d0"
START_BLOCK=39364403

polygonetl stream \
--start-block ${START_BLOCK} \
--entity-types block,transaction,log \
--log-file log.txt \
--pid-file stream.pid \
--provider-uri ${NODE_ENDPOINT} \
--output=s3://${BUCKET_NAME}
