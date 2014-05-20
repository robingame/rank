'''
'''
import sys
import datetime

class RankerException(Exception):
    '''General ranker exception'''

class NotImplementedError(RankerException):
    '''Not implemented'''

class Rank(object):
    RANK_NAME_PREFIX = 'RANK'
    SEPARATOR      = ":"
    POLICY_HIGHEST = 0
    POLICY_NEWEST  = 1

    def __init__(self):
        pass

    def setScore(self, user_id, score, rank_time=None, policy=POLICY_HIGHEST):
        raise NotImplementedError()

    def getRankByID(self, user_id, rank_index_offset=0):
        raise NotImplementedError()

    def getRankByScore(self, score, rank_index_offset=0):
        raise NotImplementedError()

    def clear(self, rank_index_offset):
        raise NotImplementedError()

    def count(self, rank_index_offset=0):
        raise NotImplementedError()


class RankType(object):

    def getType(self):
        raise NotImplementedError()

    def getIndex(self, rank_time):
        raise NotImplementedError()

_BENCH_DATE = datetime.datetime(year=2012, month=1, day=2)  #From Monday

class _DayRankType(RankType):

    def __init__(self, typename, days=1):
        self._typename = typename
        self.days = days

    def getType(self):
        return self._typename

    def getIndex(self, rank_time):
        daydiff = (rank_time - _BENCH_DATE).days
        return int(daydiff / self.days)

class MonthRankType(RankType):

    def __init__(self, typename):
        self._typename = typename

    def getType(self):
        return self._typename

    def getIndex(self, rank_time):
        years = rank_time.year - _BENCH_DATE.year
        months = rank_time.month - _BENCH_DATE.month
        return years * 12 + month

dailyRankType = _DayRankType("daily", 1)
weeklyRankType = _DayRankType("weekly", 7)
monthlyRankType = MonthRankType("monthly")

class RedisRank(Rank):

    def __init__(self, redis_client, rank_name, rank_type, max_size, policy=Rank.POLICY_HIGHEST):
        self._client = redis_client
        self._rank_name = rank_name
        self._rank_type = rank_type
        self._policy = policy
        self._max_size = max_size

    def _getPrefix(self):
        return RedisRank.SEPARATOR.join([Rank.RANK_NAME_PREFIX, self._rank_name, self._rank_type.getType()]) + RedisRank.SEPARATOR

    def getRankID(self, rank_time, rank_index_offset=0):
        index = self._rank_type.getIndex(rank_time)
        return self._getPrefix() + str(index + rank_index_offset)

    def setScore(self, user_id, score, rank_time = None, rank_index_offset=0):
        if rank_time is None:
            rank_time = datetime.datetime.now()
        rankID = self.getRankID(rank_time, rank_index_offset)
        print "setScore, rankID", rankID
        if self._policy == Rank.POLICY_HIGHEST:    #Keep the highest score for the user
            val = self._client.zscore(rankID, user_id)
            if val is None or score > val:
                self._client.zadd(rankID, user_id, score)
        elif self._policy == RedisRank.POLICY_NEWEST:   #Keep the newest score 
            self._client.zadd(rankID, user_id, score)
        totalcard = self._client.zcard(rankID) 
        if totalcard > self._max_size:
            #In ASC Order
            self._client.zremrangebyrank(rankID, 0, totalcard - RedisRanker.MAX_COUNT_MAP[ranktype] - 1)

    def incrScore(self, user_id, score, rank_time = None):
        rank_time = datetime.datetime.now()
        setname = self.getRankID(rank_time)
        return self._client.zincrby(setname, user_id, score)

    def getRankByID(self, user_id, rank_index_offset=0):
        rank_time = datetime.datetime.now()
        rankID = self.getRankID(rank_time, rank_index_offset)
        return self._client.zrevrank(rankID, user_id)

    def getScoreByID(self, user_id, rank_index_offset=0):
        rank_time = datetime.datetime.now()
        setname = self.getRankID(rank_time, rank_index_offset)
        return self._client.zscore(setname, user_id)

    def getRankByScore(self, score, rank_index_offset=0):
        rank_time = datetime.datetime.now()
        rankID = self.getRankID(rank_time, rank_index_offset)
        return self._client.zcount(rankID, '(' + str(score), '+inf')

    def clear(self, rank_index_offset):
        rank_time = datetime.datetime.now()
        rankID = self.getRankID(rank_time, rank_index_offset)
        self._client.delete(rankID)
        return rankID

    def deleteScore(self, user_id, rank_index_offset=0):
        rank_time = datetime.datetime.now()
        rankID = self.getRankID(rank_time, rank_index_offset)
        return self._client.zrem(rankID, user_id)

    def sumScores(self, rank_index_offset=0):
        setname = self.getRankID(datetime.datetime.now(), rank_index_offset)
        zcount  = self._client.zcard(setname)
        pagesize = 1000
        sumscore = 0.0
        for pageno in range((zcount/pagesize) + 1):
            start =  pageno * pagesize 
            end = start + pagesize - 1
            pagerank = self._client.zrange(setname, start, end, desc=True, withscores=True)
            sumscore += sum([s[1] for s in pagerank])
        return sumscore

    def page(self, page, page_size, rank_index_offset = 0, desc=True):
        '''
        '''
        setname = self.getRankID(datetime.datetime.now(), rank_index_offset)
        if page < 0: page = 0
        start =  page * page_size 
        end = start + page_size - 1
        return self._client.zrange(setname, start, end, desc=desc, withscores=True)

    def count(self, rank_index_offset=0):
        '''
        return the total number of user scores in the rank card
        '''
        setname = self.getRankID(datetime.datetime.now(), rank_index_offset)
        return self._client.zcard(setname)

    def list(self):
        prefix = self._getPrefix()
        return self._client.keys(prefix + "*")

    def clearTimeoutRank(self, timeout):
        curIndex = self._rank_type.getIndex(datetime.datetime.now())
        prefix = self._getPrefix()
        timeoutlist = []
        for rankid in self._client.keys(prefix + "*"):
            index = int(rankid[len(prefix):])
            if index + timeout < curIndex:
                self._client.delete(rankid)
                print "Clear timeout rank:", rankid
                timeoutlist.append(rankid)
        return timeoutlist
