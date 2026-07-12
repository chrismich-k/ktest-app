# Stage 1: Build der React SPA
FROM node:24-alpine AS build
WORKDIR /app

# Abhängigkeiten kopieren und installieren
COPY package*.json ./
RUN npm ci

# Quellcode kopieren und SPA bauen
COPY . .
RUN npm run build

# Stage 2: Bereitstellung via Nginx
FROM nginx:alpine
# Kopiere den Build-Output (passe 'dist' an, falls dein Ordner anders heißt!)
COPY --from=build /app/dist /usr/share/nginx/html
# Kopiere unsere Custom Nginx Konfiguration für den Router-Fix
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
