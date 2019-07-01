using System;
using System.Xml;
using System.Collections.Generic;
using Npgsql;
using SharpCompress.Archives;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace FiasImport
{
	class Program
	{
		/// <summary>
		/// Основной метод выполнения импорта
		/// </summary>
		/// <param name="args"> Список аргументов
		/// args[0] - Путь к файлу архива с XML файлами данных по ФИАС
		/// args[1] - Строка подключения к БД
		/// args[2] - Строка со списком названий таблиц перечисленных через запятую
		/// </param>
		static void Main(string[] args)
		{
			try
			{
				if (args == null || args.Length != 3)
					throw new ArgumentException("Количество параметров не соответствует ожидаемому. " +
						"Ожидалось три параметра: 1- Путь к архиву справочника, 2 - Строка подключения к БД, " +
						"3 - Список названий таблиц, в которые необходимо импортировать данные");

				// Путь до архива с файлами данных по ФИАС
				string fiasArchivePath = args[0];

				// Проверим, что файл архива существует
				if (!File.Exists(fiasArchivePath))
					throw new FileNotFoundException($"Не удалось найти архив с файлами данных ФИАС по адресу {fiasArchivePath}");

				// Получаем список таблиц в которые будем производить импорт данных
				List<string> tables = args[2]?.Replace(" ", "")?.Split(',')?.ToList();

				if (tables == null)
					throw new ArgumentException("Не удалось получить список таблиц для импорта. Убедитесь, что верно заданны параметры приложения");

				if (!tables.Any())
					throw new ArgumentNullException("Список таблиц для импорта данных - пуст");

				using (var connection = new NpgsqlConnection(args[1]))
				using (var command = new NpgsqlCommand())
				{
					connection.Open();
					command.Connection = connection;

					// Открываем архив с файлами данных по ФИАС
					using (var archive = ArchiveFactory.Open(fiasArchivePath))
					{
						// Цикл обхода архива с XML файлами с данными ФИАС
						foreach (var entry in archive.Entries)
						{
							// Название таблицы полученное из название файла
							// Например файл называется AS_ADDROBJ_20171217_33bb6037-d55f-49e1-bb44-b24e834a7ff5.XML
							// из него получаем ADDROBJ
							string tableName = entry.Key.Split('_')[1];

							// Импортируем только нужные таблицы
							if (!tables.Contains(tableName)) continue;

							// Открываем поток на чтение XML файла ФИАС
							using (var stream = entry.OpenEntryStream())
							{
								XmlReader reader = XmlReader.Create(stream);

								Console.WriteLine($"Запущен процесс импорта таблицы {tableName}");

								var timer = new Stopwatch();
								timer.Start();

								// Цикл обхода XML файла ФИАС
								while (reader.Read())
								{
									if (reader.NodeType == XmlNodeType.Element)
									{
										// Атрибуты и их значения представляют собой название поля в таблице и значение соответственно
										if (reader.HasAttributes)
										{
											var record = new Dictionary<string, object>();

											// Цикл обхода всех полей записи таблицы
											while (reader.MoveToNextAttribute())
											{
												string fieldName = reader.Name.ToLower();
												string fieldValue = $"'{reader.Value.ToLower()}'";

												record[fieldName] = fieldValue;
											}

											// Формируем insert запрос
											command.CommandText = FillInsertCommand(tableName.ToLower(), record);
											command.ExecuteNonQuery();
										}
									}
								}

								timer.Stop();
								Console.WriteLine($"Импорт данных таблицы {tableName} выполнен успешно. Время импорта составило: {timer.Elapsed}.");
							}
						}
					}
				}
			}
			catch (Exception ex)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine($"Критическая ошибка при выполнени импорта данных по ФИАС. Процесс импорта полностью остановлен. {ex.Message}.");
			}

			Console.ReadLine();
		}

		/// <summary>
		/// Метод формирования строки добавления записи фиас в таблицу
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="data"></param>
		/// <returns></returns>
		private static string FillInsertCommand(string tableName, Dictionary<string, object> data)
		{
			string sql = @"INSERT INTO public.{0} ({1}) VALUES ({2})";

			// Дата создания записи
			var createDate = $"'{DateTime.UtcNow.ToString("o")}'";

			data["reccreated"] = createDate;
			data["recupdated"] = createDate;
			data["recstate"] = 1;

			sql = string.Format(sql, tableName,
				string.Join(',', data.Keys).ToLower(),
				string.Join(',', data.Values).ToLower()
			);

			return sql;
		}
	}
}
