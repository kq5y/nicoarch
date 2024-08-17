FROM node:20-slim

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

ADD https://github.com/tksnnx/nicoarch-app.git /app
WORKDIR /app

RUN pnpm run -r build
CMD ["pnpm", "run", "start"]
