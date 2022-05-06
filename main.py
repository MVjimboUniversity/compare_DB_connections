# pip install pymongo
# pip install matplotlib
# pip install numpy
# pip install "pymongo[srv]"
import random
import string
import datetime
import time
import argparse
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.operations import InsertOne, UpdateOne, DeleteOne
import matplotlib.pyplot as plt
import numpy as np
from connections import LOCAL_HOST, LOCAL_PORT, DOCKER_HOST, DOCKER_PORT, CLOUD_CONNECTION


CONNECTION = {
    "local": MongoClient(LOCAL_HOST, LOCAL_PORT),
    "docker": MongoClient(DOCKER_HOST, DOCKER_PORT),
    "cloud": MongoClient(CLOUD_CONNECTION),
}

NUM_REPEATS = {
    "local": 3,
    "docker": 3,
    "cloud": 1,
}


NUM_USERS = 100
NUM_BOOKS = 1000
NUM_COMMENTS = 10000

GENRES = {
    0: 'Sci-Fi',
    1: 'Detective',
    2: 'Romance',
    3: 'Fairy Tail',
    4: 'Drama',
    5: 'Horror',
    6: 'Thriller',
    7: 'Humor'
}


def generate_random_string(length):
    letters = string.ascii_lowercase
    rand_string = ''.join(random.choice(letters) for i in range(length))
    return rand_string


def generate_alphanum_random_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    rand_string = ''.join(random.sample(letters_and_digits, length))
    return rand_string


def create_users_books(db):
    users = db['Users']
    books = db['Books']
    date = datetime.datetime(2015, 1, 1)
    for user_id in range(NUM_USERS):
        user = {
            "_id": user_id,
            "login": generate_alphanum_random_string(random.randint(8, 16)),
            "name": generate_random_string(random.randint(4, 10)),
            "surname": generate_random_string(random.randint(5, 15)),
            "email": generate_alphanum_random_string(random.randint(10, 16)) + '@gmail.com'
        }
        users.insert_one(user)
    for book_id in range(NUM_BOOKS):
        book_date = date + datetime.timedelta(days=random.randint(0, 2500))
        genres_ids = random.sample(range(len(GENRES)), k=random.randint(1, len(GENRES) - 1))
        book = {
            "_id": book_id,
            "user_id": random.randint(0, NUM_USERS - 1),
            "book_name": generate_random_string(random.randint(5, 25)),
            "book_description": generate_random_string(random.randint(20, 100)),
            "date_of_creation": book_date,
            "book_rating": random.uniform(1., 5.),
            "genres": [GENRES.get(i) for i in genres_ids]
        }
        books.insert_one(book)


def clear_all(db):
    users = db["Users"]
    books = db["Books"]
    comments = db['Comments']
    users.delete_many({})
    books.delete_many({})
    comments.delete_many({})


def generate_comments():
    date = datetime.datetime(2015, 1, 1)
    now_date = datetime.datetime.today()
    comments = []
    for comment_id in range(NUM_COMMENTS):
        user_id = random.randint(0, NUM_USERS - 1)
        book_id = random.randint(0, NUM_BOOKS - 1)
        difference_days = (now_date - date).days
        comment_date = date + datetime.timedelta(days=random.randint(1, difference_days))
        comment = {
            "_id": comment_id,
            "user_id": user_id,
            "book_id": book_id,
            "comment_text": generate_random_string(random.randint(20, 100)),
            "date_of_creation": comment_date
        }
        comments.append(comment)
    return comments


def update_generator():
    comments_text = [generate_random_string(random.randint(20, 100)) for i in range(NUM_COMMENTS)]
    return comments_text


def create_operation(db, comments_data, num_repeats):
    comments = db['Comments']
    time_for_insert_one_mean = []
    for i in range(num_repeats):
        time_for_insert_one = []
        for comment_doc in comments_data:
            start_time = time.time()
            comments.insert_one(comment_doc)
            end_time = time.time()
            try:
                time_for_insert_one.append(time_for_insert_one[-1] + (end_time - start_time))
            except Exception:
                time_for_insert_one.append(end_time - start_time)
        time_for_insert_one_mean.append(time_for_insert_one)
        comments.delete_many({})
    time_for_insert_one_mean = np.array(time_for_insert_one_mean).mean(axis=0)

    comments.delete_many({})
    time_for_insert_many_mean = []
    for i in range(num_repeats):
        time_for_insert_many = []
        for comment_cut in range(1, NUM_COMMENTS+1, 500):
            comments_insert = comments_data[:comment_cut]
            start_time = time.time()
            comments.insert_many(comments_insert)
            end_time = time.time()
            comments.delete_many({})
            time_for_insert_many.append(end_time - start_time)
        start_time = time.time()
        comments.insert_many(comments_data)
        end_time = time.time()
        time_for_insert_many.append(end_time - start_time)
        time_for_insert_many_mean.append(time_for_insert_many)
        comments.delete_many({})
    time_for_insert_many_mean = np.array(time_for_insert_many_mean).mean(axis=0)

    comments.delete_many({})
    time_for_insert_bulk_mean = []
    for i in range(num_repeats):
        time_for_insert_bulk = []
        for comment_cut in range(1, NUM_COMMENTS + 1, 500):
            comments_insert = comments_data[:comment_cut]
            requests = []
            for comment_insert in comments_insert:
                requests.append(InsertOne(comment_insert))
            start_time = time.time()
            comments.bulk_write(requests)
            end_time = time.time()
            comments.delete_many({})
            time_for_insert_bulk.append(end_time - start_time)
        requests = []
        for comment_insert in comments_data:
            requests.append(InsertOne(comment_insert))
        start_time = time.time()
        comments.bulk_write(requests)
        end_time = time.time()
        time_for_insert_bulk.append(end_time - start_time)
        time_for_insert_bulk_mean.append(time_for_insert_bulk)
        comments.delete_many({})
    time_for_insert_bulk_mean = np.array(time_for_insert_bulk_mean).mean(axis=0)

    res = [(range(1, len(time_for_insert_one_mean) + 1), time_for_insert_one_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_insert_many_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_insert_bulk_mean)]
    return res


def read_operation(db, comments_data, num_repeats):
    comments = db['Comments']
    time_for_read_all_mean = []
    for i in range(num_repeats):
        comments.delete_many({})
        time_for_read_all = []
        for comment_doc in comments_data:
            comments.insert_one(comment_doc)
            start_time = time.time()
            comments.find({})
            end_time = time.time()
            time_for_read_all.append(end_time-start_time)
        time_for_read_all_mean.append(time_for_read_all)
    time_for_read_all_mean = np.array(time_for_read_all_mean).mean(axis=0)

    time_for_read_queries_mean = []
    for i in range(num_repeats):
        time_for_read_queries = []
        for comment_id in range(NUM_COMMENTS):
            start_time = time.time()
            comments.find({"_id": {"$lte": comment_id}})
            end_time = time.time()
            time_for_read_queries.append(end_time - start_time)
        time_for_read_queries_mean.append(time_for_read_queries)
    time_for_read_queries_mean = np.array(time_for_read_queries_mean).mean(axis=0)

    res = [(range(1, len(time_for_read_all_mean) + 1), time_for_read_all_mean),
           (range(1, len(time_for_read_queries_mean) + 1), time_for_read_queries_mean)]
    return res


def update_operation(db, comments_data, comments_text, num_repeats):
    comments = db['Comments']
    comments.insert_many(comments_data)
    time_for_update_one_mean = []
    for i in range(num_repeats):
        time_for_update_one = []
        for comment_id in range(NUM_COMMENTS):
            start_time = time.time()
            comments.update_one({"_id": comment_id}, {"$set": {"comment_text": comments_text[comment_id]}})
            end_time = time.time()
            try:
                time_for_update_one.append(time_for_update_one[-1] + (end_time - start_time))
            except Exception:
                time_for_update_one.append(end_time-start_time)
        time_for_update_one_mean.append(time_for_update_one)
    time_for_update_one_mean = np.array(time_for_update_one_mean).mean(axis=0)

    time_for_update_many_mean = []
    for i in range(num_repeats):
        time_for_update_many = []
        for comment_id in range(1, NUM_COMMENTS+1, 500):
            start_time = time.time()
            comments.update_many({"_id": {"$lt": comment_id}}, {"$set": {"comment_text": comments_text[comment_id]}})
            end_time = time.time()
            time_for_update_many.append(end_time-start_time)
        start_time = time.time()
        comments.update_many({"_id": {"$lte": NUM_COMMENTS-1}},
                             {"$set": {"comment_text": comments_text[NUM_COMMENTS-1]}})
        end_time = time.time()
        time_for_update_many.append(end_time - start_time)
        time_for_update_many_mean.append(time_for_update_many)
    time_for_update_many_mean = np.array(time_for_update_many_mean).mean(axis=0)

    time_for_update_bulk_mean = []
    for i in range(num_repeats):
        time_for_update_bulk = []
        for comment_id in range(1, NUM_COMMENTS+1, 500):
            requests = []
            for i in range(0, comment_id):
                requests.append(UpdateOne({"_id": i},
                                          {"$set": {"comment_text": comments_text[i]}}))
            start_time = time.time()
            comments.bulk_write(requests)
            end_time = time.time()
            time_for_update_bulk.append(end_time - start_time)
        requests = []
        for i in range(NUM_COMMENTS):
            requests.append(UpdateOne({"_id": i},
                                      {"$set": {"comment_text": comments_text[i]}}))
        start_time = time.time()
        comments.bulk_write(requests)
        end_time = time.time()
        time_for_update_bulk.append(end_time - start_time)
        time_for_update_bulk_mean.append(time_for_update_bulk)
    time_for_update_bulk_mean = np.array(time_for_update_bulk_mean).mean(axis=0)

    res = [(range(1, len(time_for_update_one_mean) + 1), time_for_update_one_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_update_many_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_update_bulk_mean)]
    return res


def delete_operation(db, comments_data, num_repeats):
    comments = db['Comments']
    time_for_delete_one_mean = []
    for i in range(num_repeats):
        time_for_delete_one = []
        comments.insert_many(comments_data)
        for comment_id in range(NUM_COMMENTS):
            start_time = time.time()
            comments.delete_one({"_id": comment_id})
            end_time = time.time()
            try:
                time_for_delete_one.append(time_for_delete_one[-1] + (end_time - start_time))
            except Exception:
                time_for_delete_one.append(end_time - start_time)
        time_for_delete_one_mean.append(time_for_delete_one)
    time_for_delete_one_mean = np.array(time_for_delete_one_mean).mean(axis=0)

    time_for_delete_many_mean = []
    for i in range(num_repeats):
        comments.insert_many(comments_data)
        time_for_delete_many = []
        for comment_id in range(1, NUM_COMMENTS+1, 500):
            start_time = time.time()
            comments.delete_many({"_id": {"$lt": comment_id}})
            end_time = time.time()
            time_for_delete_many.append(end_time - start_time)
            comments.insert_many(comments_data[:comment_id])
        start_time = time.time()
        comments.delete_many({"_id": {"$lte": NUM_COMMENTS - 1}})
        end_time = time.time()
        time_for_delete_many.append(end_time - start_time)
        time_for_delete_many_mean.append(time_for_delete_many)
    time_for_delete_many_mean = np.array(time_for_delete_many_mean).mean(axis=0)

    time_for_delete_bulk_mean = []
    for i in range(num_repeats):
        comments.insert_many(comments_data)
        time_for_delete_bulk = []
        for comment_id in range(1, NUM_COMMENTS+1, 500):
            requests = []
            for i in range(0, comment_id):
                requests.append(DeleteOne({"_id": i}))
            start_time = time.time()
            comments.bulk_write(requests)
            end_time = time.time()
            time_for_delete_bulk.append(end_time - start_time)
            comments.insert_many(comments_data[:comment_id])
        requests = []
        for i in range(NUM_COMMENTS):
            requests.append(DeleteOne({"_id": i}))
        start_time = time.time()
        comments.bulk_write(requests)
        end_time = time.time()
        time_for_delete_bulk.append(end_time - start_time)
        time_for_delete_bulk_mean.append(time_for_delete_bulk)
    time_for_delete_bulk_mean = np.array(time_for_delete_bulk_mean).mean(axis=0)

    res = [(range(1, len(time_for_delete_one_mean) + 1), time_for_delete_one_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_delete_many_mean),
           (range(1, NUM_COMMENTS + 2, 500), time_for_delete_bulk_mean)]
    return res


def create_compare():
    try:
        fig, axes = plt.subplots(nrows=1, ncols=3, constrained_layout=True)
        fig.set_size_inches(10, 5)
        axes[0].set(title='insert_one')
        axes[1].set(title='insert_many')
        axes[2].set(title='bulk(insert_one)')
        for ax in axes.flat:
            ax.grid()
            ax.set_xlabel('Number of data')
            ax.set_ylabel('Time(sec)')

        for name, connection in CONNECTION.items():
            connection.admin.command('ismaster')
            db = connection['DB']

            clear_all(db)
            # create_users_books(db)
            comments_data = generate_comments()
            times = create_operation(db, comments_data, NUM_REPEATS.get(name))

            for ax, time in zip(axes.flat, times):
                ax.plot(time[0], time[1], label=name)
                ax.legend()
        plt.show()
    except ConnectionFailure:
        pass


def read_compare():
    try:
        fig, axes = plt.subplots(nrows=1, ncols=2, constrained_layout=True)
        fig.set_size_inches(10, 5)
        axes[0].set(title='find({})')
        axes[1].set(title='find({_id: {}})')
        for ax in axes.flat:
            ax.grid()
            ax.set_xlabel('Number of data')
            ax.set_ylabel('Time(sec)')

        for name, connection in CONNECTION.items():
            connection.admin.command('ismaster')
            db = connection['DB']

            clear_all(db)
            # create_users_books(db)
            comments_data = generate_comments()
            times = read_operation(db, comments_data, NUM_REPEATS.get(name))

            for ax, time in zip(axes.flat, times):
                ax.plot(time[0], time[1], label=name)
                ax.legend()
        plt.show()
    except ConnectionFailure:
        pass


def update_compare():
    try:
        fig, axes = plt.subplots(nrows=1, ncols=3, constrained_layout=True)
        fig.set_size_inches(10, 5)
        axes[0].set(title='update_one')
        axes[1].set(title='update_many')
        axes[2].set(title='bulk(update_one)')
        for ax in axes.flat:
            ax.grid()
            ax.set_xlabel('Number of data')
            ax.set_ylabel('Time(sec)')

        for name, connection in CONNECTION.items():
            connection.admin.command('ismaster')
            db = connection['DB']

            clear_all(db)
            # create_users_books(db)
            comments_data = generate_comments()
            comments_text = update_generator()
            times = update_operation(db, comments_data, comments_text, NUM_REPEATS.get(name))

            for ax, time in zip(axes.flat, times):
                ax.plot(time[0], time[1], label=name)
                ax.legend()
        plt.show()
    except ConnectionFailure:
        pass


def delete_compare():
    try:
        fig, axes = plt.subplots(nrows=1, ncols=3, constrained_layout=True)
        fig.set_size_inches(10, 5)
        axes[0].set(title='delete_one')
        axes[1].set(title='delete_many')
        axes[2].set(title='bulk(delete_one)')
        for ax in axes.flat:
            ax.grid()
            ax.set_xlabel('Number of data')
            ax.set_ylabel('Time(sec)')

        for name, connection in CONNECTION.items():
            connection.admin.command('ismaster')
            db = connection['DB']

            clear_all(db)
            # create_users_books(db)
            comments_data = generate_comments()
            times = delete_operation(db, comments_data, NUM_REPEATS.get(name))

            for ax, time in zip(axes.flat, times):
                ax.plot(time[0], time[1], label=name)
                ax.legend()
        plt.show()
    except ConnectionFailure:
        pass


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('operation')
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    operation = args.operation
    operations_dict = {
        "create": create_compare,
        "read": read_compare,
        "update": update_compare,
        "delete": delete_compare
    }
    operations_dict.get(operation)()


if __name__ == '__main__':
    main()
