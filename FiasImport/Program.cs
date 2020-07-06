using System;
using System.Xml;
using System.Collections.Generic;
using Npgsql;
using SharpCompress.Archives;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

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
		/// args[3] - onlyCalcAddress - если указан этот параметр, то произойдет только рассчет адреса в уже существующих данных. Вставки данных не будет.
		/// </param>
		static void Main(string[] args)
		{
			try
			{
				if (args == null || args.Length < 3)
					throw new ArgumentException("Количество параметров не соответствует ожидаемому. " +
						"Ожидалось три параметра: 1- Путь к архиву справочника, 2 - Строка подключения к БД, " +
						"3 - Список названий таблиц, в которые необходимо импортировать данные, " +
						"4 - опционально onlyCalcAddress");

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

				var onlyCalcAddress = false;
				var onlyCalcLexem = false;

				if (args.Length == 4 && args[3].ToLower() == "onlycalcaddress")
					onlyCalcAddress = true;
				else if (args.Length == 4 && args[3].ToLower() == "onlycalclexem")
					onlyCalcLexem = true;

				if (!onlyCalcAddress && !onlyCalcLexem)
				{
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
									int recordCount = 0;

									// Цикл обхода XML файла ФИАС
									while (reader.Read())
									{
										if (reader.NodeType == XmlNodeType.Element)
										{
											//Атрибуты и их значения представляют собой название поля в таблице и значение соответственно
											if (reader.HasAttributes)
											{
												var record = new Dictionary<string, object>();

												//Цикл обхода всех полей записи таблицы
												while (reader.MoveToNextAttribute())
												{
													string fieldName = reader.Name.ToLower();
													string fieldValue = $"'{reader.Value.Replace("'", "").Replace('«', '"').Replace('»', '"')}'";

													record[fieldName] = fieldValue;
												}

												var actstatus = record["actstatus"].ToString().Replace("'", ""); // статус актуальности ФИАС
												if (!record.ContainsKey("currstatus") || string.IsNullOrWhiteSpace(record["currstatus"]?.ToString()) || record["currstatus"]?.ToString() == "''")
													record["currstatus"] = "'0'";
												if (!record.ContainsKey("operstatus") || string.IsNullOrWhiteSpace(record["operstatus"]?.ToString()) || record["operstatus"]?.ToString() == "''")
													record["operstatus"] = "'0'";

												var currstatus = record["currstatus"].ToString().Replace("'", ""); // статус актуальности КЛАДР

												//Пишем в БД только актуальные адресные объекты, actstatus == 1 и currstatus == 0
												if (tableName.ToLower() == "addrobj" && int.Parse(actstatus) == 1 && int.Parse(currstatus) == 0)
												{
													//Формируем insert запрос
													command.CommandText = FillInsertCommand(tableName.ToLower(), record);

													try
													{
														command.ExecuteNonQuery();
													}
													catch (Exception ex)
													{
														throw new Exception($"Ошибка вставки в таблицу {tableName.ToLower()}. {ex.Message}");
													}

													recordCount++;
												}
											}
										}
									}

									timer.Stop();

									Console.WriteLine($"Импорт данных таблицы {tableName} выполнен успешно. Время импорта составило: {timer.Elapsed}. Импортировано {recordCount} записей.");

								}
							}
						}
					}

					Console.WriteLine("Импорт базы ФИАС выполнен успешно!");
				}

				if (!onlyCalcLexem)
				{
					// Перерассчет полного адреса для таблички адресов
					var t = new Stopwatch();

					try
					{
						Console.WriteLine($"Выполняем рассчет полного адреса для каждого адресного объекта");
						t.Start();
						int updRowCount = CalculateAndUpdateFullAddress(args[1]);
						t.Stop();
						Console.WriteLine($"Рассчитали полный адрес для {updRowCount} записей. С момента начала вычисления прошло: {t.Elapsed}.");
					}
					catch (Exception ex)
					{
						t.Stop();
						throw new Exception($"Ошибка при вычислении полных адресов. Время, прошедшее с момента запуска: {t.Elapsed}. Ошибка: {ex.Message}.");
					}
				}

				try
				{
					CalculateAndUpdateFullAddressSearch(args[1]);
				}
				catch(Exception ex)
				{
					throw new Exception($"Ошибка при вычислении лексем. {ex.Message}");
				}

				
			}
			catch (Exception ex)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine($"Критическая ошибка при выполнени импорта данных по ФИАС. Процесс импорта полностью остановлен. {ex.Message}.");
				Console.ForegroundColor = ConsoleColor.White;
			}

			Console.WriteLine("Нажмите любую клавишу для завершения работы программы...");
			Console.ReadKey();
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

			data["recid"] = $"'{Guid.NewGuid()}'";
			data["reccreated"] = createDate;
			data["recupdated"] = createDate;
			data["recstate"] = 1;

			sql = string.Format(sql, "rdev___fias_addrobj",
				string.Join(',', data.Keys).ToLower(),
				string.Join(',', data.Values)
			);

			return sql;
		}

		/// <summary>
		/// Рассчет полных адресов ФИАС
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns>Количество обновленных записей</returns>
		private static int CalculateAndUpdateFullAddress(string connectionString)
		{
			var connection = new NpgsqlConnection(connectionString);

			try
			{
				using (var command = new NpgsqlCommand("UPDATE rdev___fias_addrobj ao SET fulladdress = get_addrobj_fulladdress(ao.aoguid)"))
				{
					connection.Open();
					command.Connection = connection;

					int count = command.ExecuteNonQuery();
					return count;
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка при вычислении полных адресов. Ошибка: {ex.Message}.");
				return -1;
			}
			finally
			{
				connection.Close();
			}
		}

		#region Разбиение полного адреса на фрагменты

		private static void CalculateAndUpdateFullAddressSearch(string connectionString)
		{
			var stop = new Stopwatch();

			try
			{
				Console.WriteLine($"Выполняем рассчет лексем на основе полного адреса");

				stop.Start();

				while (true)
				{
					var records = GetRecords(connectionString);
					if (records.Count() <= 0)
					{
						Console.WriteLine("Список записей для обработки пуст.");
						break;
					}
					else
					{
						// Разбиваем адрес на фрагменты
						var updRecords = new Dictionary<Guid, string>();
						foreach (var record in records)
						{
							var fasValue = ParseAddressString(record.Value);
							updRecords.Add(record.Key, fasValue);
						}

						UpdateRecords(connectionString, updRecords);
					}
				}
			}
			catch(Exception ex)
			{
				Console.WriteLine($"Критическая ошибка при рассчете лексем. Message: {ex.Message}. InnerMessage: {ex.InnerException.Message}");
			}
			finally
			{
				stop.Stop();
			}

			Console.WriteLine($"Рассчет лексем закончен: длительность составила {stop.Elapsed}");
		}

		static Dictionary<Guid, string> GetRecords(string connectionString)
		{
			var records = new Dictionary<Guid, string>();

			using (var connection = new NpgsqlConnection(connectionString))
			{
				connection.Open();
				using (var cmd = new NpgsqlCommand())
				{
					cmd.Connection = connection;
					cmd.CommandText = "select recid, fulladdress from rdev___fias_addrobj where fulladdress_search IS NULL limit 20";

					var reader = cmd.ExecuteReader();

					while (reader.Read())
						records.Add(Guid.Parse(reader[0].ToString()), reader[1].ToString());
				}
			}

			return records;
		}

		static string ParseAddressString(string fullAddress)
		{
			var addressParts = fullAddress.Split(',');
			var sb = new StringBuilder();

			foreach (var addressPart in addressParts)
			{
				string part = addressPart.Replace(',', ' ');
				part = part.Trim();
				part = part.ToLower();
				//part = RemoveStopWords(part);
				part = DivideWord(part);

				sb.Append(part);
				sb.Append(" ");
			}

			return RemoveDublicate(sb.ToString().Trim());
		}

		static string DivideWord(string phrase)
		{
			phrase = phrase.Replace('.', ' ');
			phrase = phrase.Trim();
			var phraseParts = phrase.Split(' ');

			var sb = new StringBuilder();

			foreach (var phrasePart in phraseParts)
			{
				var part = phrasePart.Trim();
				int wordSymbolCount = part.Length;

				var sb2 = new StringBuilder();

				if (wordSymbolCount <= 3)
				{
					sb.Append($"{part} ");
				}
				else
				{
					for (int i = 0; i < wordSymbolCount; i++)
					{
						var lexema = part.Substring(0, i + 1);

						if (lexema.Length > 1)
							sb2.Append($"{part.Substring(0, i + 1)} ");
					}

					sb.Append(sb2.ToString());
				}
			}

			return sb.ToString().Trim();
		}

		static string RemoveDublicate(string s)
		{
			var parts = s.Split(" ").Reverse<string>();

			var sb = new StringBuilder();
			var localParts = parts.ToList<string>();

			foreach (var part in parts)
			{
				int counter = 0;

				foreach (var item in localParts)
				{
					if (item == part) counter++;
				}

				if (counter > 1)
					localParts.Remove(part);
			}

			sb.AppendJoin(" ", localParts.Reverse<string>());
			return sb.ToString().Trim();
		}

		static int UpdateRecords(string connectionString, Dictionary<Guid, string> updRecords)
		{
			using (var connection = new NpgsqlConnection(connectionString))
			{
				connection.Open();
				using (var cmd = new NpgsqlCommand())
				{
					cmd.Connection = connection;

					int countUpdRecord = 0;
					foreach (var updRecord in updRecords)
					{
						cmd.CommandText = $"update rdev___fias_addrobj set fulladdress_search = '{updRecord.Value}' where recid = '{updRecord.Key}'";
						countUpdRecord += cmd.ExecuteNonQuery();
					}

					return countUpdRecord;
				}
			}
		}

		#endregion
	}
}
