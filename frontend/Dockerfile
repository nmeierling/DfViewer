FROM node:22.20.0-alpine AS build
WORKDIR /app
RUN npm install -g npm@11.6.2
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npx ng build --configuration=production

FROM nginx:alpine
COPY --from=build /app/dist/frontend/browser /usr/share/nginx/html
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
ENV NGINX_ENVSUBST_FILTER='^(FRONTEND_PORT|BACKEND_PORT)$'
ENV FRONTEND_PORT=4200
ENV BACKEND_PORT=8080
