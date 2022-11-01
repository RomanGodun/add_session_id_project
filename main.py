import pandas as pd

file_path = "your/path/to/file/data.csv"


def add_sid(group, local_sid):

    sids = []

    local_sid += 1                                              # увеличиваем пришедший глобальный sid на 1 тк для новой группы он должен быть новым
    group = group.sort_values(by=["timestamp"])
    timestamps = (group["timestamp"].astype("int64").values)    # получаем список отсортированных таймстэмпов в виде числа

    cursor = timestamps[0]

    for timestamp in timestamps:

        if (timestamp - cursor) <= 180000000000:                # если разница между взятым таймстэмпом и курсором меньше 3-х минут то берем текущий sid
            sids.append(local_sid)
        else:                                                   # если нет то на этом таймстэмпе начинается новая сессия с новым sid
            local_sid += 1
            sids.append(local_sid)

        cursor = timestamp                                      # делаем курсором текущим таймстэмп

    group["session_id"] = sids

    return local_sid, group


def add_sessions_to_csv(df, file_path):

    global_sid = 0

    gb = df.groupby("customer_id")

    customer_ids = df["customer_id"].unique()                                    # получаем список уникальных customer_id, тк gb.get_group() работает оптимальнее по памяти чем цикл по gb.groups
    customer_ids.sort()

    global_sid, group = add_sid( gb.get_group( customer_ids[0]) , global_sid )   # добавляем session_id к первой группе
    group.to_csv(file_path, index=False)                                # перезаписываем файл первой обработанной группой                                   

    for c_id in customer_ids[1:]:

        global_sid, group = add_sid(gb.get_group(c_id), global_sid)
        group.to_csv(file_path, mode="a", index=False, header=False)    # дозаписываем группу к общему датасету


if __name__ == "__main__":

    df = pd.read_csv(
        file_path,
        dtype={"customer_id": "uint32", "product_id": "uint64"},
        parse_dates=["timestamp"],
    )

    add_sessions_to_csv(df, file_path)
