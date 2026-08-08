# FOUNDATION-001 — Project Documentation Framework

**Estado:** Vigente
**Última actualización:** ver historial de Git

## Propósito

Este documento define cómo se organiza la documentación de este proyecto y por qué. No es un estándar corporativo importado — es una estructura deliberadamente liviana, ajustada a un ritmo de desarrollo de 1-3 horas por semana y a una etapa de validación temprana (pre-cliente, pre-negocio confirmado).

## Principio rector

La documentación existe para acelerar el aprendizaje, no para demostrar exhaustividad. Cada documento debe responder una pregunta concreta que afecta una decisión real del proyecto. Si un documento no cambia ninguna decisión, no se escribe.

Esto descarta deliberadamente, por ahora:
- Documentación de mercado extensa (40+ páginas)
- Personas de usuario completas y múltiples
- Technology Radar corporativo
- Whitepapers de estrategia estilo consultora
- Cualquier documento cuyo principal valor sea "verse profesional" en vez de reducir incertidumbre real

## Estructura de carpetas

```
docs/
├── 00-foundation/     ← el marco mismo (este documento)
└── 01-discovery/      ← evidencia y decisiones de validación temprana
```

Carpetas futuras (no creadas todavía, se agregan cuando haya contenido real que las justifique):
- `02-product/` — cuando exista definición de producto validada
- `03-architecture/` — decisiones de arquitectura que trasciendan el código mismo (ADRs)
- `04-engineering/` — estándares de ingeniería, cuando el equipo crezca más allá de una persona

## Capas del proyecto

El proyecto se entiende en tres capas distintas, que no deben mezclarse:

1. **Capa técnica (portafolio):** arquitectura, pipelines, calidad de datos, código — demuestra capacidad de construir.
2. **Capa de experimento de producto:** publicación, comunidad, observación de señales — responde si a alguien le importa.
3. **Capa de negocio:** ICP, pricing, clientes — responde si alguien pagaría.

Cada documento de este framework pertenece a una sola capa. Mezclar capas en un mismo documento es la forma más común de confundir "construimos algo impresionante" con "validamos un negocio".

## Convención de nomenclatura

`[PREFIJO]-[NÚMERO]-[nombre-descriptivo].md`

Ejemplos: `SD-001` (Startup Discovery), `HB-001` (Hypothesis Board). El número permite versionar sin perder historial (`SD-002` sería una revisión mayor de la primera).
