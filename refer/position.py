import random

def position_generator(year: int, month: int) -> int:
    """Return first_position for the given year and month."""
    year_start = 139838 - (year - 2026) * 40000

    # For now divide_number is fixed to 6; keep random line if you want variability later.
    # divide_number = random.randint(1, 2) * 6
    divide_number = 6

    unit = 40000 // 12
    month_start = (month - 12 // divide_number) * unit
    month_end = month * unit

    month_number = random.randint(month_start, month_end)
    first_position = year_start - month_number
    return first_position

def position_generator_el_gordo(year: int, month: int) -> int:
    """Return first_position for the given year and month."""
    year_start = 31625 - (year - 2026) * 10000

    # For now divide_number is fixed to 6; keep random line if you want variability later.
    # divide_number = random.randint(1, 2) * 6
    divide_number = 6

    unit = 10000 // 12
    month_start = (month - 12 // divide_number) * unit
    month_end = month * unit

    month_number = random.randint(month_start, month_end)
    first_position = year_start - month_number
    return first_position

def position_generator_la_primitiva(year: int, month: int) -> int:
    """Return first_position for the given year and month."""
    year_start = 139838 - (year - 2025) * 5000

    # For now divide_number is fixed to 6; keep random line if you want variability later.
    # divide_number = random.randint(1, 2) * 6
    divide_number = 6

    unit = 5000 // 12
    month_start = (month - 1) * unit
    month_end = month * unit

    month_number = random.randint(month_start, month_end)
    first_position = year_start - month_number
    return first_position

if __name__ == "__main__":
    fp = position_generator(2026, 4)
    fp1 = position_generator_el_gordo(2026,4)
    print("first_position:", fp)
    print("first_position_el_gordo:", fp1)