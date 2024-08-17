FROM node:20-slim as base

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

ADD https://github.com/tksnnx/nicoarch-app.git /app
WORKDIR /app

ENV NODE_ENV=production

RUN pnpm install --frozen-lockfile --prod=false
RUN pnpm build

CMD ["pnpm", "start"]
