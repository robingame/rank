from redis import Redis
from random import randint
from rank import *

'''
MAX_DAILY_COUNT  = 200000
MAX_WEEKLY_COUNT = 300000
MAX_MONTH_COUNT  = 500000
MAX_LIFE_COUNT   = 1000000
'''

redis_client = Redis("localhost", 6379, 0)
dailyrank = RedisRank(redis_client, "game-1", dailyRankType, 2000000)

for user_id in xrange(0, 100):
    #score = randint(1, 10000)
    score = user_id + 10
    dailyrank.setScore(user_id, score)

for user_id in [100, 99, 98, 5, 0]:
    print user_id, dailyrank.getRankByID(user_id), dailyrank.getScoreByID(user_id)

dailyrank.count()


page = 0 
page_size = 10
dailyrank.page(page, page_size)

user_id  = 0
dailyrank.deleteScore(user_id)
print dailyrank.getRankByID(user_id)

dailyrank.incrScore(user_id, 18)
print dailyrank.getScoreByID(user_id)

#insert score for yesterday's rank
for user_id in xrange(0, 100):
    score = 100 - user_id
    dailyrank.setScore(user_id, score, rank_index_offset = -1)


for user_id in [100, 99, 98, 5, 0]:
    print user_id, dailyrank.getRankByID(99, rank_index_offset = -1)


print dailyrank.list()


'''
After all, you need a cron job to delete the timeout ranks
'''
dailyrank.clearTimeoutRank(2)
