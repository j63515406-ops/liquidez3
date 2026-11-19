# Bot de Liquidez (independiente) - BingX -> Telegram

Entra solo cuando el precio esta sobre una zona de liquidez importante y muestra fuerza.

## Setups
- **SOUP (Turtle Soup)**: la vela barre la zona (maximos/minimos iguales) y cierra de vuelta dentro, con volumen y mecha de rechazo.
- **SOUP+1 (Plus One)**: la vela barre y cierra fuera; la siguiente recupera el nivel con vela a favor.
- **REBOTE**: rechazo en la zona sin barrido (mecha >= 50% + volumen). Setup mas debil (-10 de calidad).

## Zonas (arriba, abajo e internas)
Cumulos de pivotes sin barrer, puntuados 0-100: toques (30) + recencia (10) + volumen en pivotes (15)
+ confluencia con liquidaciones estimadas (30) + extremo de 20 velas / zona externa (15).
Se etiquetan como externa alta, externa baja o interna (medio del rango).

## Liquidaciones estimadas
No hay datos publicos de posiciones reales. Se usa el modelo habitual de los heatmaps: volumen por vela
repartido en apalancamientos (10/25/50/100x), precio de liquidacion = entrada * (1 -/+ 0.95/L),
descartando niveles que el precio ya consumio. Es una estimacion, no un dato real.

## Extras del indicador de TradingView (UT Bot + LinReg + S/R + panel)
Suman hasta +20 puntos a la calidad (el tope sigue siendo 100). Todos se pueden apagar con variables.
- **UT Bot** (trailing stop ATR): +4 si va a favor de la operacion, +2 mas si giro en las ultimas 3 velas. `LQ_UT_REQUIRE=true` lo convierte en filtro duro.
- **LinReg** (cierre suavizado vs su media de 7): +3 si el impulso va a favor.
- **S/R**: +3 si la zona coincide con el maximo/minimo de 100-500 velas.
- **Tendencia multi-TF** (EMA9 vs EMA21 en 3 TF superiores): +2 por TF a favor, -1 por TF en contra (de -4 a +6). `LQ_HTF_MIN_ALIGNED=2` exige al menos 2 a favor.
- **Maximo/minimo de ayer**: +2 si la zona coincide.
- **Longevidad**: el mensaje indica cuanto tiempo lleva la zona sin barrerse.
- **Panel** en el mensaje: volatilidad, ATR, volumen, VWAP, MACD, Stoch, RSI, UT, LinReg, tendencias y nivel de ayer.
Como suman puntos, por eso `LQ_MIN_QUALITY` es 72 por defecto.

## Mejoras v2
- **Vela fresca**: si BingX aun no publico la vela recien cerrada, reintenta (`FETCH_RETRIES`) y no evalua con la anterior; antes la senal se perdia.
- **R:R neto de comisiones** (`LQ_MIN_NET_RR=1.2`): descarta operaciones donde la comision (0,1%) se come el beneficio. Se muestra "R:R 2.0 (neto 1.7)".
- **Riesgo correlacionado**: una posicion por moneda entre timeframes y maximo `LQ_MAX_SAME_SIDE` por lado (evita 10 shorts a la vez en un dump de BTC).
- **Filtro contra-tendencia total**: no opera si los 3 TF superiores (EMA9/21) van todos en contra (`LQ_BLOCK_FULL_COUNTER`).
- **TF superiores solo con velas cerradas** y "ayer" = ultimo dia cerrado (antes usaba la vela en curso).
- **Breakeven opcional** (`LQ_BE_TRIGGER_R`): al llegar a N*R el bot avisa "mover SL a breakeven" y la posicion virtual lo aplica.
- **Persistencia** (`STATE_FILE`): posiciones abiertas, cooldowns y operaciones del dia sobreviven a reinicios. En Railway añade un Volume o se pierde al redesplegar.
- **Mas rapido**: prefiltro de volumen exacto (mismo resultado) y analisis fuera del bucle de red.
- **Resumen diario** con acierto/esperanza por tramo de calidad y motivos de salida, para ajustar `LQ_MIN_QUALITY` con datos.

## Extras v3 (del indicador MapleStax CBC)
- **CBC** (`LQ_USE_CBC`): estado alcista si la vela cierra por encima del maximo previo, bajista si cierra por debajo del minimo previo (estricto). +3 si va a favor, +2 mas si giro en las ultimas 2 velas; en el primer TF superior +2. `LQ_CBC_REQUIRE=true` lo convierte en filtro duro.
- **Niveles de sesion** (`LQ_USE_SESSIONS`): Opening Range (primeros `LQ_OR_MINUTES` min) de New York 09:30, Londres 03:00 y Asia 20:00 (hora ET, con horario de verano) de las 2 ultimas sesiones completas, mas maximo/minimo del ultimo RTH de NY completo (YDH/YDL). +3 si la zona coincide.
- **EMA200 / VWAP del dia** (`LQ_USE_DYN`): +2 si la zona coincide con alguno. **Numeros redondos** (`LQ_USE_ROUND`, apagado por defecto): +2.
- **Tope de extras** (`LQ_EXTRAS_CAP=22`): todos los extras juntos suman como maximo 22 puntos; sin tope la calidad se satura y deja de discriminar.
- **Informe "Por extra"**: acierto y esperanza de las operaciones CON y SIN cada extra. Es la forma de decidir con datos cuales se quedan y cuales se apagan.
- **Ya estaban integrados** del otro script: UT Bot, LinReg, S/R de 10-1000 velas, zonas de longevidad, panel, tendencia multi-TF y maximo/minimo de ayer.

## Backtest (medir antes de cambiar parametros)
`python liquidez_bot.py backtest` descarga historico de BingX y simula con la MISMA logica que el modo real.
Variables: `BT_DAYS` (14), `BT_TOP` (40 monedas por volumen) o `BT_SYMBOLS`. Usa los mismos `LQ_*` del entorno, asi que
puedes comparar configuraciones: p. ej. `LQ_BE_TRIGGER_R=1 python liquidez_bot.py backtest` contra la version sin BE.
Los TF superiores se reconstruyen agregando las velas base (el EMA9/21 diario/semanal necesita 25+ velas: usa `BT_DAYS>=30` para incluirlos). Limitaciones: entrada al cierre de vela, SL antes que TP si caen en la misma vela.
Con pocas operaciones (<100) los porcentajes de acierto no son significativos.

## v4: resultados del backtest 30 dias y ajustes
Base (5m+15m, 40 monedas, calidad 72): 235 ops · 31% acierto · esperanza +0.01% · PF 1.03 · total +2.80% · drawdown 29.81%.
Con ganancia media +1.51% y perdida media -0.65% el punto de equilibrio es ~30% de acierto: la base estaba justo en equilibrio.
- **5m pierde en los dos lados** (-25.09%, 159 ops, 26% acierto) y tambien ANTES de comisiones (~-9%): no es solo un problema de fees. **15m gana** (+27.88%, 76 ops, 41%). `TIMEFRAMES=15m`.
- **Breakeven a 1R empeora** (+2.80% -> -13.06%): muchas operaciones tocan 1R y vuelven a la entrada antes del objetivo. Se queda apagado.
- **Mas calidad no es mejor**: <80 +0.06%, 80-88 -0.06%, 88+ -0.23%. No subir `LQ_MIN_QUALITY`.
- **Extras con esperanza peor CON que SIN**: LinReg, UT, CBC HTF, EMA200/VWAP. Hipotesis: confirman un movimiento que ya ocurrio y la entrada llega tarde. `LQ_EXTRA_WEIGHTS=LinReg=0,UT=0,EMA200/VWAP=0,CBC HTF=0` (con 30-40 ops por extra es evidencia debil: confirmar con 60+ dias).
- **Ayer** (maximo/minimo de ayer) fue lo unico claramente positivo (18 ops, 67%): muestra pequena, se mantiene.

Nuevas herramientas de backtest: `BT_CACHE` (historico reutilizable), `BT_WORKERS` (procesos), `BT_OUTPUT` (guarda el informe), `BT_TRADES_OUT` (CSV de operaciones),
`BT_PORTFOLIO` (aplica 1 posicion por moneda y `LQ_MAX_SAME_SIDE`, como el modo real), desglose **por semana** y **robustez** (resultado sin las 5 mejores operaciones).
`LQ_EXTRA_WEIGHTS` pondera cada extra sin perder su medicion. El informe "Por extra" ahora incluye `EMA200 favor` y `Zona extremo20` (solo estadistica, ya puntuan en la formula base).
Como comparar A/B: `set BT_CACHE=bt_cache` y lanzar dos veces cambiando solo una variable; la segunda usa los mismos datos.

## Gestion
SL: mas alla del extremo del barrido (+ buffer ATR). TP: siguiente zona de liquidez opuesta importante
(o R:R fijo). Salida: TP, SL o tiempo maximo. Cada senal abre una posicion virtual y el bot avisa al salir.

## Resumen diario
Operaciones, % acierto, esperanza por operacion, profit factor, drawdown maximo, por tipo / timeframe / lado.

## Despliegue
Segundo servicio en Railway sobre el MISMO repo: Settings -> Custom Start Command: `python liquidez_bot.py`
Variables propias del servicio (ver .env.liquidez.example). Comparte requirements.txt con el otro bot.
