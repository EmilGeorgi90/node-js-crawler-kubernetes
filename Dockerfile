# --- Build stage ---
FROM node:22-alpine AS build
WORKDIR /app

# Enable pnpm and cache deps
RUN corepack enable && corepack prepare pnpm@latest --activate
COPY package.json ./
COPY pnpm-lock.yaml ./
RUN pnpm fetch

# Add sources and build
COPY tsconfig.json ./
COPY src ./src
COPY tools ./tools
RUN pnpm install --offline --frozen-lockfile \
 && pnpm run build

# --- Runtime stage ---
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

# Install system Chromium & fonts so Puppeteer can launch it
RUN apk add --no-cache \
      chromium \
      nss \
      freetype \
      harfbuzz \
      ca-certificates \
      ttf-freefont

# Tell Puppeteer where Chromium lives
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Copy app
COPY --from=build /app/package.json ./
COPY --from=build /app/pnpm-lock.yaml ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

# Default entrypoint (override per service in compose/k8s)
CMD ["node", "dist/src/worker.js"]
