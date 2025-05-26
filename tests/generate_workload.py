import random
import string
import sys

# --- 配置 ---
TARGET_SIZE_MB = 105  # 目标文件大小 (MB)，略大于100MB以确保满足条件
KEY_PREFIX = "key"
KEY_NUM_DIGITS = 7  # key中数字部分的位数 (例如 0000001)
AVG_VALUE_LENGTH = 1000  # value的平均长度
INSERT_PROBABILITY = 0.9  # 插入操作的概率 (0.9 表示 90%)

# 用于生成复杂value的字符集 (不包含空格，以兼容您C++代码中的解析方式)
COMPLEX_VALUE_CHARS = string.ascii_letters + string.digits + "_-=+[]{}|;:,.<>/?!@#$%^&*()"
# --- 结束配置 ---

def generate_key(idx: int) -> str:
    """生成一个简单的key"""
    return f"{KEY_PREFIX}{idx:0{KEY_NUM_DIGITS}d}"

def generate_complex_value(length: int) -> str:
    """生成一个指定长度的复杂value"""
    if length <= 0:
        return "default_value"
    return ''.join(random.choice(COMPLEX_VALUE_CHARS) for _ in range(length))

def main():
    # 估算总操作数以达到目标文件大小
    key_len = len(KEY_PREFIX) + KEY_NUM_DIGITS
    # I<space>key<space>value<newline>
    avg_insert_line_size = 1 + 1 + key_len + 1 + AVG_VALUE_LENGTH + 1
    # D<space>key<newline>
    avg_delete_line_size = 1 + 1 + key_len + 1
    
    avg_line_size_estimate = (INSERT_PROBABILITY * avg_insert_line_size) + \
                             ((1 - INSERT_PROBABILITY) * avg_delete_line_size)
    
    target_bytes = TARGET_SIZE_MB * 1024 * 1024
    num_operations = int(target_bytes / avg_line_size_estimate)

    if num_operations <= 0:
        print("Error: Calculated number of operations is zero or negative. Check configuration.", file=sys.stderr)
        sys.exit(1)

    print(f"# Configuration:", file=sys.stderr)
    print(f"# Target size: {TARGET_SIZE_MB} MB", file=sys.stderr)
    print(f"# Avg value length: {AVG_VALUE_LENGTH}", file=sys.stderr)
    print(f"# Insert probability: {INSERT_PROBABILITY:.2f}", file=sys.stderr)
    print(f"# Estimated operations: {num_operations}", file=sys.stderr)
    print(f"# Estimated key length: {key_len}", file=sys.stderr)
    print(f"# Estimated avg line size: {avg_line_size_estimate:.2f} bytes", file=sys.stderr)
    print(f"# Estimated total size: {(num_operations * avg_line_size_estimate) / (1024*1024):.2f} MB", file=sys.stderr)
    print(f"# --- Starting data generation ---", file=sys.stderr)

    # 存储已插入且可供删除的key
    available_for_delete = []
    key_id_counter = 0 # 用于生成唯一的key ID

    for op_idx in range(num_operations):
        # 决定是插入还是删除
        is_insert = random.random() < INSERT_PROBABILITY

        if not is_insert and available_for_delete:
            # 执行删除操作
            key_to_delete_idx = random.randrange(len(available_for_delete))
            key_to_delete = available_for_delete.pop(key_to_delete_idx) # 从可删除列表中移除
            print(f"D {key_to_delete}")
        else:
            # 执行插入操作 (或者因为没有可删除的key而转为插入)
            new_key = generate_key(key_id_counter)
            key_id_counter += 1
            
            # value长度可以有些随机性
            current_value_length = AVG_VALUE_LENGTH + random.randint(-int(AVG_VALUE_LENGTH * 0.2), int(AVG_VALUE_LENGTH * 0.2))
            current_value_length = max(10, current_value_length) # 确保value不会太短

            value = generate_complex_value(current_value_length)
            print(f"I {new_key} {value}")
            available_for_delete.append(new_key) # 将新插入的key添加到可删除列表

        if (op_idx + 1) % (num_operations // 20) == 0 and op_idx > 0 : # 每5%打印一次进度
             progress = (op_idx + 1) / num_operations * 100
             print(f"# Generated {op_idx + 1}/{num_operations} operations ({progress:.1f}%)...", file=sys.stderr)

    print(f"# --- Finished generating {num_operations} operations ---", file=sys.stderr)
    print(f"# Total unique keys generated for insert: {key_id_counter}", file=sys.stderr)
    print(f"# Keys remaining in 'available_for_delete' pool: {len(available_for_delete)}", file=sys.stderr)

if __name__ == "__main__":
    main()

# python ./generate_workload.py > workload.txt