import csv

# 文件名: convert_ratings_to_train_format.py

# 读取原始评分数据文件
input_file = '../data/original_ratings.csv'  # 原始评分数据文件的名称
output_file = '../data/train.txt'  # 转换后的文件名称
rating_threshold = 3.5  # 定义正向互动的评分阈值

# 用于存储用户ID及其正向互动的车位ID列表
user_interactions = {}

# 读取原始评分数据文件
with open(input_file, 'r') as infile:
    reader = csv.reader(infile)
    next(reader)  # 跳过标题行
    for row in reader:
        # 跳过空行
        if not row or len(row) != 3:
            print(f"Skipping line due to incorrect format: {row}")
            continue

        user_id, parking_spot_id, rating = row
        user_id = int(user_id)
        parking_spot_id = int(parking_spot_id)
        rating = float(rating)  # 假设评分是浮点数

        # 如果评分大于等于阈值，记录这个互动
        if rating >= rating_threshold:
            if user_id not in user_interactions:
                user_interactions[user_id] = []
            user_interactions[user_id].append(parking_spot_id)

# 将结果写入转换后的文件
with open(output_file, 'w') as outfile:
    for user_id, parking_spots in user_interactions.items():
        parking_spots_str = ','.join(map(str, parking_spots))
        outfile.write(f"{user_id}\t{parking_spots_str}\n")

print("ok")