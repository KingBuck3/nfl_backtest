# nfl_backtest

My goal with this project is to predict fantasty points scored by National Football League (NFL) players each week. I have pulled down
historical data, such as passing defense stats, point spread and point totals predicted by sportsbooks, and fantasy points scored by
players.

The qb_data file has all of the functions I have built to wrangle and merge datasets. After running the main() program, the output results
in a .csv file indexed by each player for a given week. The .csv file includes the stats of the defense that the player is facing, the game
pread game total, and the player's team total.

With this historical data in hand, I plan to evaluate various scikitlearn models to evaluate which model best fits/predicts future fantasy
points scored by NFL players.
