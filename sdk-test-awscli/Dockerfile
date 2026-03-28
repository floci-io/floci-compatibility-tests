FROM amazon/aws-cli:latest

RUN yum install -y python3 unzip >/dev/null 2>&1 \
    && curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip \
    && cd /tmp && unzip -qo awscli.zip \
    && ./aws/install --update >/dev/null 2>&1 \
    && rm -rf /tmp/aws /tmp/awscli.zip

WORKDIR /app

COPY test_all.sh .

ENV FLOCI_ENDPOINT=http://floci:4566

ENTRYPOINT ["/bin/bash", "test_all.sh"]
