# HB-001 — Hypothesis Board

**Estado:** Vigente — versión consolidada final (H1-H9)
**Capa:** Experimento de producto / Negocio
**Regla:** cada hipótesis pasa de ❓ a ✅/❌ solo con evidencia real (investigación de fuentes o señales observadas), no con intuición.

---

## Tabla resumen

| # | Hipótesis | Área | Estado |
|---|---|---|---|
| H1 | Integrar una fuente de datos nueva es un dolor frecuente y caro | Problema | Señal indirecta fuerte, sin testimonio local directo |
| H2 | Están dispuestos a pagar por resolverlo | Dolor económico | Mixta — presión regulatoria a favor, capital de riesgo escaso como alerta |
| H3 | El dolor principal es gobernanza/cumplimiento, no solo velocidad | Dolor específico | Más sólida de las 9, con matiz de un CEO peruano (Rextie) que apunta a otro dolor |
| H4 | El CTO decide sin proceso largo | Comprador | Sin evidencia local — inferencia razonable de literatura general de fintech |
| H5 | El proyecto público genera credibilidad suficiente para abrir conversaciones (no mide clientes directamente) | Señal de mercado | En observación pasiva — se valida con la publicación del MVP |
| H6 | Fintechs de préstamos/pagos, 20-100 empleados, Lima, es el segmento correcto | Segmento | Tamaño de mercado confirmado, afinar por estabilidad/financiamiento reciente |
| H7 | No existen alternativas maduras que resuelvan esto suficientemente bien | Competencia | Parcialmente refutada en forma amplia (Fivetran/Airbyte existen) — sobrevive en el ángulo de gobernanza normativa local |
| H8 | El monto que pagarían sostiene el negocio | Pricing | Sin confirmar con datos específicos — benchmarks regionales sugieren viabilidad |
| H9 | Confiarían datos sensibles a un proveedor sin trayectoria comprobada | Confianza | No investigable — mitigada por diseño (entrada vía piloto/auditoría gratuita, no venta directa) |

---

## Detalle por hipótesis

Ver documento complementario de detalle expandido para el razonamiento completo, evidencia citada y qué movería a cada hipótesis de estado. Resumen de qué evidencia movería cada una:

- **H1/H3:** una conversación directa, o una reacción no solicitada de alguien con perfil relevante tras publicar
- **H2/H8:** que alguien pregunte por precio o cómo contratar sin que se lo hayas ofrecido
- **H4:** observar el cargo de quien contacta tras publicar, y si menciona necesitar aprobación de alguien más
- **H5:** cualquier interacción no solicitada tras publicar (a favor); silencio de 3-4 semanas en múltiples canales (en contra, no definitivo)
- **H6:** observar si las reacciones vienen más de pagos o de préstamos específicamente
- **H7:** que un prospecto técnico pregunte "¿por qué no Fivetran/Airbyte?" y la respuesta de gobernanza local lo convenza
- **H9:** que alguien acepte una auditoría/piloto gratuito, sin necesitar confianza suficiente para pagar de entrada

## Dimensión transversal (no es una hipótesis nueva)

**"Why now":** ver sección 3 de SD-001. Alimenta directamente H1 y H3 — el timing regulatorio y de mercado es una de las señales más fuertes recopiladas hasta ahora.

## Próxima revisión

Este documento se actualiza después de la Semana 8 del roadmap (observación de señales tras publicación), no antes. Evitar revisar o "reinvestigar" hipótesis fuera de ese ciclo — el riesgo activo en esta etapa es sobre-investigar en vez de ejecutar.
