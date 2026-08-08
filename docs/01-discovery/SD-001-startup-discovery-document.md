# SD-001 — Startup Discovery Document

**Estado:** En validación activa
**Capa:** Experimento de producto / Negocio

## 1. Problema

Fintechs y bancos medianos en LatAm (foco inicial: Lima, Perú) enfrentan datos dispersos, integración lenta de fuentes nuevas, y riesgo regulatorio por gobernanza de datos insuficiente. El resultado: decisiones de riesgo basadas en información poco confiable, y exposición a sanciones normativas.

## 2. Para quién (ICP inicial)

Fintechs de pagos o préstamos digitales, 20-100 empleados, Lima — con preferencia por empresas que ya demostraron cierta estabilidad (2+ años operando, o financiamiento reciente), dado el alto índice de mortalidad temprana en el segmento (19.7% en 2021-2023) y la escasez de capital de riesgo disponible en el ecosistema peruano.

## 3. Por qué ahora ("Why now")

- Crecimiento del sector fintech peruano ~17% anual (237 empresas a dic. 2024)
- SBS actualizó su reglamento de sanciones en abril 2026, elevando a "muy grave" las fallas de protección de datos (20 a 100+ UIT)
- Sandbox regulatorio y Open Banking/Open Finance avanzando hacia 2026-2027, exigiendo mayor trazabilidad de datos entre entidades
- La industria regional (Deloitte, Fintech Américas) ya identifica modernización de datos como prioridad estratégica 2026, no solo de TI
- El propio mercado se mueve hacia agentes de IA para datos — timing razonable para una solución en esa dirección

## 4. Qué NO vamos a construir (por ahora)

Explícitamente fuera de alcance hasta validar el problema con evidencia real:
- Data Mesh corporativo completo
- Terraform multi-cliente / infraestructura multi-tenant
- Streaming en tiempo real (Kafka)
- Portal, marketplace, SDK público
- Agente de IA con Vector DB/LLMs (Fase 1, no MVP)
- Documentación de mercado extensa, personas múltiples, Technology Radar

## 5. Diferenciación frente a alternativas existentes

Herramientas como Fivetran y Airbyte ya resuelven el movimiento genérico de datos, con años de ventaja y adopción masiva. La propuesta de valor de este proyecto no compite ahí — se enfoca específicamente en la capa que esas herramientas no cubren: gobernanza normativa peruana (Ley N° 29733, reglamento SBS) y detección de fraude con reglas de contexto local (ej. umbrales de fraccionamiento de la UIF-Perú).

## 6. Definición de MVP técnico público (v1)

Un pipeline Bronze-Silver-Gold funcional, con:
- Arquitectura documentada (diagrama de arquitectura en el README)
- Caso demostrable con datos reales (PaySim), incluyendo métricas de detección medidas, no estimadas
- Gobernanza básica (masking, RLS, categorías Ley 29733)
- Evidencia visual (dashboard, lineage)
- Documentación técnica sólida

No incluye (ver sección 4) infraestructura multi-cliente ni el agente de IA.

## 7. Qué significa éxito en los próximos 90 días

No es ingreso ni clientes firmados — es **evidencia real que mueva las hipótesis del Hypothesis Board (ver HB-001) de "no validada" a "validada" o "descartada"**, específicamente:
- MVP técnico publicado y con métricas reales documentadas
- Al menos una señal de interés no solicitada (comentario técnico específico, mensaje, pregunta puntual) de alguien con perfil relevante (fintech/banca/datos)
- Un precio de referencia definido (no necesariamente probado con un cliente)

## 8. Roadmap hasta MVP público

Ver documento de roadmap semanal (Semanas 1-8) para el detalle ejecutable. Resumen de fases:

1. Medición real con datos (PaySim) e integración de diagrama
2. Lógica de fraude con línea base y versión mejorada
3. Diferenciación explícita frente a alternativas + ingesta multi-formato
4. Automatización y trazabilidad de decisiones (tests, CI/CD, ADRs)
5. Definición de pricing de referencia
6-7. Publicación en canales técnicos y LinkedIn, observación pasiva
8. Revisión de evidencia y decisión de siguiente fase

## 9. Restricción operativa activa

El fundador mantiene empleo actual en banca y requiere perfil bajo — no se realizan entrevistas directas ni contacto activo con prospectos en esta etapa. La validación se basa en investigación de fuentes públicas y observación pasiva de señales tras la publicación del proyecto.
