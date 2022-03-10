--ЦТЕ с данными по каждому рейсу
with ttl as ( 
	select q5.flight_number
			,q5.fact_off_dttm
			,q5.fact_on_dttm
			,q5.fact_off_dttm::date as fact_off_dt
			,q5.fact_off_dttm::time as fact_off_time
			,q5.fact_on_dttm::date as fact_on_dt
			,q5.fact_on_dttm::time as fact_on_time
			,q5.dest_airp_code
			,q5.arrival_airp_code
			,q5.estim_off_time::time
			,q5.estim_on_time::time
			,q5.dest_airp_name
			,q5.dest_country
			,ref.name as arrival_airp_name
			,ref.country as arrival_airp_country
	from (
		select q4.* 
				,ref.name as dest_airp_name
				,ref.country as dest_country
		from (
			select q3.*
					,sc.dest_airport_code as dest_airp_code
					,sc.origin_airport_code as arrival_airp_code
					,sc.off_block_time as estim_off_time 
					,sc.on_block_time as estim_on_time
			from (
				select *
				from (
					select *
							,LEAD(q1.fact_off_dttm, 1) over (PARTITION by flight_number
													order by fact_off_dttm) as fact_on_dttm
					from (
						select flight_number
								,movement_type
								,cast(cast(to_date(t.date, 'dd.mm.yyyy') as text) || ' ' || cast(t.time as text) as TIMESTAMP) as fact_off_dttm 
						from traffic_report t
						) as q1
					) as q2
				where movement_type = 'departure' --and flight_number = 9876
				) as q3
			left join schedule sc on q3.flight_number = sc.flight_number
			) as q4
		left join ref_airports ref on q4.dest_airp_code = ref.id
		) as q5
	left join ref_airports ref on q5.arrival_airp_code = ref.id
),
--ЦТЕ со статусами опоздания рейсов
late_reason as (
	select q6.*
			,CASE
				WHEN q6.arrival_status = 'delayed' and q6.dep_status = 'late_dept'
					THEN 'Поздний вылет'
					ELSE 
						(CASE
		            		WHEN q6.arrival_status = 'delayed' and (q6.dep_status = 'early_dep' or q6.dep_status ISNULL) 
		               			THEN 'Сильный встречный ветер'
		               			ELSE 'Прилетел вовремя'
		               	END)
			END as delayed_reason
	
	from (
		SELECT *
				,CASE
		            WHEN (fact_on_time - estim_on_time) > '00:00:00' or (fact_on_time - estim_on_time) < '00:00:00'
		               THEN 'delayed'
		               ELSE 'ontime'
		       	END as arrival_status
		       	,CASE
		            WHEN (fact_off_time - estim_off_time) < '00:00:00' 
		               THEN 'early_dep'
		               ELSE
		               		(CASE
		            			WHEN (fact_off_time - estim_off_time) > '00:00:00' 
		               				THEN 'late_dept'
		               				ELSE null
		               		END) 	
		       	END as dep_status
		from ttl 
		) as q6
),
--Справочник дней недели
dayOfWeek as (
	select DISTINCT to_char(ttl.fact_off_dttm::date, 'Day') as day_name
	from ttl
)

--Вывод общего отчета по вылетам
select  flight_number as Номер_рейса
		,fact_off_dttm as Фактическая_дата_и_время_вылета
		,fact_on_dttm as Фактическая_дата_и_время_прилета
		,fact_off_dt as Фактическая_дата_вылета
		,fact_off_time as Фактическое_время_вылета
		,fact_on_dt as Фактическая_дата_прилета
		,fact_on_time as Фактическое_время_прилета
		,estim_off_time as Время_вылета_по_расписанию
		,estim_on_time as Время_прилета_по_расписанию
		,dest_airp_code as Код_аэропорта_вылета
		,arrival_airp_code as Код_аэропорта_прилета
		,dest_airp_name as Аэропорт_вылета
		,dest_country as Страна_вылета
		,arrival_airp_name as Аэропорт_прилета
		,arrival_airp_country as Страна_прилета
from ttl
order by 1 asc