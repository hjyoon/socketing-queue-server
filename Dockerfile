FROM node:20-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts \
  && npm cache clean --force

COPY --chown=node:node index.js ./
COPY --chown=node:node dist ./dist

EXPOSE 3000

USER node

CMD ["node", "index.js"]
