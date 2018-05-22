import sys
import time
import redis
import random
import uuid


def match(uid, score):
    print("matching")
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    match_queue = "rankQueue"
    r = get_redis()
    lock = "match_lock"
    lock_timeout = 2;

    try:
        locked = get_lock(r, lock, lock_timeout)
        if locked:
            print("get lock success")

            # ZRANGEBYSCORE ranges1 35 +inf limit 0 1
            val1 = get_max_score_element(r, match_queue, score)
            # ZRANGEBYSCORE ranges1 -inf 35 limit len-1 1
            val2 = get_min_score_element(r, match_queue, score)
            print("-------*-------")
            print(val1)
            print(val2)
            print("-------*-------")
            val = return_suitable_element(val1, val2, r, match_queue, score)

            print(type(val))
            if val:
                print(val[0])
                # del
                rem_element_from_queue(r, match_queue, val)
                return val
            else:
                room_id = create_room(r)
                print(room_id)
                if add_room_into_queue(r, match_queue, room_id, count_user_score(uid)):
                    return room_id
                else:
                    return -1
        else:
            print("do not get lock")

    except:
        print("Unexpected error:", sys.exc_info()[0])

    finally:
        freed_lock(r, lock, locked)


def return_suitable_element(ele1, ele2, conn, queue_name, score):
    r = conn
    if ele1:
        if ele2:
            if r.zscore(queue_name, ele1[0]) - score > r.zscore(queue_name, ele2[0]) - score:
                return ele2
            else:
                return ele1
        else:
            return ele1
    else:
        if ele2:
            return ele2
        else:
            return None


def get_min_score_element(conn, queue_name, score):
    r = conn
    # ZCOUNT ranges1 -inf 35
    length = r.zcount(queue_name, "-inf", score)
    print(length)
    if length:
        print("try reverse get")
        # ZRANGEBYSCORE ranges1 -inf 35 limit 2 1
        val = r.zrangebyscore(queue_name, "-inf", score, length - 1, 1)
        return val
    else:
        return None


def get_max_score_element(conn, queue_name, score):
    return conn.zrangebyscore(queue_name, score, "+inf", 0, 1)


def rem_element_from_queue(conn, queue_name, element):
    print("del element.")
    print(element)
    print(
        conn.zrem(queue_name, element[0])
    )


def add_room_into_queue(conn, queue_name, room_id, score):
    # print("add room=" + room_id + " score=" + score)
    # test
    r = conn
    return r.zadd(queue_name, room_id, score)


def create_room(conn):
    # test
    r = conn
    return r.incr("rankIncr")


def count_user_score(uid):
    # test
    return random.randint(1, 500)


def get_lock(conn, lock, lock_timeout):
    print("try get lock")
    r = conn
    identifier = uuid.uuid4()

    if r.setnx(lock, identifier):
        r.expire(lock, lock_timeout)
        return identifier

    return False


def freed_lock(conn, lock, identifier):
    print("freed lock");
    r = conn
    pipe = r.pipeline(True)

    try:
        pipe.watch(lock)
        if pipe.get(lock) == identifier:
            pipe.multi()
            pipe.delete(lock)
            pipe.execute()
            return True

        pipe.unwatch()

    except redis.exceptions.WatchError:
        pass

    return False


def get_redis():
    # test
    return redis.Redis(host='127.0.0.1', port=6379)


if __name__ == '__main__':
    print("start match")
    print(
        match(888666, 298)
    )
