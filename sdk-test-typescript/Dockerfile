FROM node:22-slim
WORKDIR /app

COPY package.json .
RUN npm install --no-fund --no-audit

COPY test-all.mjs .

ENV FLOCI_ENDPOINT=http://floci:4566

ENTRYPOINT ["node", "test-all.mjs"]
