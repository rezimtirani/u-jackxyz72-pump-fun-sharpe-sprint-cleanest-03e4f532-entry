# Pump.fun Sharpe Sprint — Cleanest Paper Bot Wins

[![make.fun · u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532/rezimtirani/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532-entry/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532/rezimtirani/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532-entry/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)

A paper-trading bot for a 6-hour pump.fun forward replay, ranked by hourly
Sharpe ratio. It chases small, steady, low-variance gains instead of
moonshots: it buys only bonding curves that survived the launch dump and
show accelerating buy pressure from several distinct wallets, sizes each
buy small (one buy per coin, no adds), and exits fast on a take-profit, a
tight stop loss, a trailing stop, fading flow, or a single big sell. At most five positions are open at once
and stakes stay small, so no single coin can swing the hourly return much.

## How it works

`main.ts` (Node 22, standard library only, no dependencies) speaks the
`agent-fun/replay/v2` protocol on stdin/stdout: the harness sends `hello`,
then one `batch` per slot, then `done`; the bot answers each batch with
exactly one `reply` carrying its orders. Decisions come only from the
batch stream — no clock, no environment, no randomness — so two runs on
the same data produce byte-identical action logs.

```sh
node main.ts
```

Grade it locally with the kit (process driver; Docker is not installed here):

```sh
node <kit>/cli/makefun.mjs grade-local --kit <kit> --bot . --driver process
```

## License

MIT — see [LICENSE](LICENSE).

<!-- SPDX-License-Identifier: MIT -->
