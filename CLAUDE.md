# DFViewer

Data viewer tool for browsing, filtering, and comparing S3/local parquet/CSV data.

## Tech Stack
- **Backend**: Spring Boot 3, Kotlin, Gradle, Java 21, DuckDB
- **Frontend**: Angular 19, PrimeNG (Aura theme), TypeScript
- **Deployment**: Docker Compose

## Project Structure
```
backend/     - Spring Boot Kotlin backend (Gradle)
frontend/    - Angular 19 SPA
docker-compose.yml
```

## Build Commands
```bash
# Backend
cd backend && ./gradlew build -x test
cd backend && ./gradlew bootRun

# Frontend
cd frontend && npm install
cd frontend && npx ng serve
cd frontend && npx ng build

# Docker
docker compose up --build
```

## Conventions
- Backend package: `com.pmd.dfviewer`
- DuckDB tables for imported data: `ds_{id}`
- Registry table: `dataset_registry`
- REST API under `/api/`
- Frontend components use standalone Angular components with inline templates for small components
- PrimeNG Aura theme — use PrimeNG components (p-table, p-button, etc.)
