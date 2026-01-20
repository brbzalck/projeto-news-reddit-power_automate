# Inteligência Artificial: Monitor Global de Narrativas e Opniões

![Status](https://img.shields.io/badge/Status-Finished-success)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![PowerBI](https://img.shields.io/badge/PowerBI-Desktop-yellow)
![FastAPI](https://img.shields.io/badge/API-FastAPI-009688)

> **Monitoramento geopolítico em tempo real sobre Inteligência Artificial.**
> Uma solução de **Engenharia de Dados End-to-End** que compara como a grande mídia e a opinião pública das duas maiores potências mundiais (EUA e China) reagem aos avanços da IA.

---

## 📸 Dashboard Preview

![Dashboard Overview](assets/dashboard_overview.png)

---

## 🏗️ Arquitetura do Projeto

O projeto utiliza um **pipeline ETL autônomo**, orquestrado localmente, com coleta, processamento, persistência e visualização desacoplados.

```mermaid
graph TD
    subgraph "1. Extração (Scrapers)"
        A[WSJ] -->|Playwright| E(Orquestrador Python)
        B[People's Daily] -->|Playwright| E
        C[X / Twitter] -->|Playwright| E
        D[Weibo] -->|Playwright| E
    end

    subgraph "2. Processamento & Armazenamento"
        E -->|Limpeza & Tradução| F[(SQLite Database)]
        F <-->|Query| G[FastAPI Server]
    end

    subgraph "3. Visualização & Automação"
        G -->|JSON Data| H[Power BI Dashboard]
        I[Power Automate Desktop] -->|Trigger| E
        I -->|Trigger| H
    end

