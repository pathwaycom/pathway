FROM node:latest as build

ARG API_PORT
ENV API_PORT=$API_PORT

ARG MAPBOX_TOKEN
ENV MAPBOX_TOKEN=$MAPBOX_TOKEN

WORKDIR /app
COPY package*.json /app/
RUN npm install
COPY ./ /app/
RUN npm run prod

FROM nginx:stable-alpine
COPY --from=build /app/dist /usr/share/nginx/html
# RUN rm /etc/nginx/conf.d/default.conf
# COPY nginx.conf /etc/nginx/conf.d
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
