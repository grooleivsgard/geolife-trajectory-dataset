from part1 import Part1
from part2 import Part2

# Upload data to database
part1 = Part1()
part1.upload_data()

# Perform queries to database
part2 = Part2()
part2.execute_tasks(task_nums=1)
# OR
part2.execute_tasks(task_nums=[1, 2, 6, 9])
# OR
part2.execute_tasks(task_nums=range(1, 13))  # 13 is exclusive

