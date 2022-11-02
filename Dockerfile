FROM node:16.17 AS base
MAINTAINER J. Scott Smith <scott@newleafsolutionsinc.com>
# Following Best Practices and guidelines at:
#   https://nodejs.org/en/docs/guides/nodejs-docker-webapp/
#   https://github.com/nodejs/docker-node/blob/master/docs/BestPractices.md
RUN groupmod -g 2000 node \
  && usermod -u 2000 -g 2000 node
WORKDIR /home/node/app
# Install dependencies
COPY package.json /home/node/app
COPY package-lock.json /home/node/app
# Best practice: run with NODE_ENV set to production
ENV NODE_ENV production
RUN npm install

# Linting layer, won't make it into production
FROM base AS linter
ENV NODE_ENV development
RUN npm install
COPY . /home/node/app
RUN npm run lint

# Testing layer, won't make it into production
FROM linter AS tester
RUN npm run test
RUN date > .buildtime
#
# Build stage skipped for node image, since it would require dev dependencies
#

FROM base AS prod
# Best practice: run as user 'node'
USER node
EXPOSE 3040
# Copy source files; relies on .dockerignore
COPY --from=tester /home/node/app/.buildtime .buildtime
COPY . /home/node/app

# Best practice: bypass the package.json's start
CMD [ "node", "./node_modules/moleculer/bin/moleculer-runner.js", "./src/services/**/*.service.js" ]
