import pandas as pd
import sqlite3

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

def csv_to_sql(path, tab_name):
	df = pd.read_csv(path)
	df.to_sql(tab_name, con = conn, if_exists='replace', index=False)

def show_data(source):
	print('_-'*20)
	print(source)
	print('_-'*20)
	cursor.execute(f'select * from {source}')
	for row in cursor.fetchall():
		print(row)

def init_auto_hist():
	cursor.execute('''
		CREATE TABLE if not exists hist_auto(
			id integer primary key autoincrement,
			model varchar(128),
			transmission varchar(128),
			body_type varchar(128),
			drive_type varchar(128),
			color varchar(128),
			production_year integer,
			auto_key integer,
			engine_capacity real,
			horsepower integer,
			engine_type varchar(128),
			price integer,
			milage integer,
			deleted_flg integer default 0,
			start_dttm datetime default current_timestamp,
			end_dttm datetime default (datetime('2999-12-31 23:59:59'))
		)
	''')

	cursor.execute(''' 
		CREATE VIEW if not exists v_auto as
			select
				model, transmission, body_type, drive_type, color,
            	production_year, auto_key, engine_capacity, horsepower,
            	engine_type, price, milage
			from hist_auto
			where deleted_flg = 0
			and current_timestamp between start_dttm and end_dttm
	''')

def create_new_rows():

	cursor.execute('''
		CREATE TABLE new_rows_tmp as
			SELECT
			 t1.* 
			from auto_tmp t1
			left join v_auto t2
			on t1.auto_key = t2.auto_key
			where t2.auto_key is null 
	''')

def create_deleted_rows():
	cursor.execute('''
		CREATE TABLE deleted_rows_tmp as
			SELECT
			 t1.* 
			from v_auto t1
			left join auto_tmp t2
			on t1.auto_key = t2.auto_key
			where t2.auto_key is null 
	''')

def create_changed_rows():
	cursor.execute('''
		CREATE TABLE changed_rows_tmp as
			SELECT
			 t2.* 
			from v_auto t1
			inner join auto_tmp t2
			on t1.auto_key = t2.auto_key
			and (t1.model != t2.model or
				t1.transmission != t2.transmission or
				t1.body_type != t2.body_type or
				t1.drive_type != t2.drive_type or
				t1.color != t2.color or
				t1.production_year != t2.production_year or
				t1.engine_capacity != t2.engine_capacity or
				t1.horsepower != t2.horsepower or
				t1.engine_type != t2.engine_type or
				t1.price != t2.price or
				t1.milage != t2.milage) 
		''')

def update_hist_auto():
	# добавление новых записей
	cursor.execute('''
		INSERT INTO hist_auto (
            model, transmission, body_type, drive_type, color,
            production_year, auto_key, engine_capacity, horsepower,
            engine_type, price, milage)
		SELECT 
            model, transmission, body_type, drive_type, color,
			production_year, auto_key, engine_capacity, horsepower,
			engine_type, price, milage
		FROM new_rows_tmp
	''')
	# изменение времени старых измененных записей
	cursor.execute('''
		UPDATE hist_auto
		SET end_dttm = datetime('now', '-1 second')
		WHERE auto_key in (select auto_key from changed_rows_tmp)
		and end_dttm = datetime('2999-12-31 23:59:59')
	''')

	# добавление измененных записей как новых
	cursor.execute('''
		INSERT INTO hist_auto (
            model, transmission, body_type, drive_type, color,
			production_year, auto_key, engine_capacity, horsepower,
			engine_type, price, milage)
		SELECT 
            model, transmission, body_type, drive_type, color,
			production_year, auto_key, engine_capacity, horsepower,
			engine_type, price, milage
		FROM changed_rows_tmp
	''')

	# добавление удаленных записей с изменением времени

	cursor.execute('''
		UPDATE hist_auto
		SET end_dttm = datetime('now', '-1 second')
		WHERE auto_key in (select auto_key from deleted_rows_tmp)
		and end_dttm = datetime('2999-12-31 23:59:59')
	''')

	# добавление удаленных записей как новых с признаком удаления
	cursor.execute('''
		INSERT INTO hist_auto (
            model, transmission, body_type, drive_type, color,
			production_year, auto_key, engine_capacity, horsepower,
			engine_type, price, milage, deleted_flg)
		SELECT 
            model, transmission, body_type, drive_type, color,
			production_year, auto_key, engine_capacity, horsepower,
			engine_type, price, milage, 1
		FROM deleted_rows_tmp
	''')
	conn.commit()

# удаление временных таблиц
def drop_tmp_tables():
	cursor.execute('DROP TABLE if exists auto_tmp')
	cursor.execute('DROP TABLE if exists new_rows_tmp')
	cursor.execute('DROP TABLE if exists changed_rows_tmp')
	cursor.execute('DROP TABLE if exists deleted_rows_tmp')
	
init_auto_hist()
drop_tmp_tables()
csv_to_sql('store/data_3.csv', 'auto_tmp')



create_new_rows()
create_changed_rows()
create_deleted_rows()

update_hist_auto()

show_data('auto_tmp')
show_data('new_rows_tmp')
show_data('changed_rows_tmp')
show_data('deleted_rows_tmp')
show_data('hist_auto')







