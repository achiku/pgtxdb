FROM postgres:12

ENV POSTGRES_DB=pgtxdbtest \
    POSTGRES_USER=pgtxdbtest \
    POSTGRES_HOST_AUTH_METHOD=trust

EXPOSE 5432

