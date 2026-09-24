# Container image for the document embedding worker.
#
# Built from the repository root:
#   docker build -f new-lambdas/infra/container/Dockerfile -t embedding-batch new-lambdas
#
# The application source is copied from functions/ml/ into the task root, so any
# module the handler imports must live inside functions/ml/.

FROM public.ecr.aws/lambda/python:3.12

# Tokenizer and model assets are large enough that this function cannot be
# deployed as a zip archive.
COPY infra/container/requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

COPY functions/ml/ ${LAMBDA_TASK_ROOT}/

CMD [ "embedding_batch.lambda_handler" ]
