package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// MinuteKey 分钟级聚合键
type MinuteKey struct {
	Date      string
	Minute    string
	Code      string
	Name      string
	Direction string
}

// DayKey 天级聚合键
type DayKey struct {
	Date      string
	Code      string
	Name      string
	Direction string
}

// Stats 存储累加数据
type Stats struct {
	TotalVolume float64
	TotalAmount float64
}

func main() {
	// // wjwy
	// GenerateCsv("./data/wjwy.csv", "./data/wjwy_agg_minute.csv", "./data/wjwy_agg_day.csv")
	// GenerateTdxDayCode("./data/wjwy_agg_day.csv", "./data/wjwy_tdx_day.txt")
	// GenerateTdxMinCode("./data/wjwy_agg_minute.csv", "./data/wjwy_tdx_min.txt")
	// //92kebi
	// GenerateCsv("./data/92kebi.csv", "./data/92kebi_agg_minute.csv", "./data/92kebi_agg_day.csv")
	// GenerateTdxDayCode("./data/92kebi_agg_day.csv", "./data/92kebi_tdx_day.txt")
	// GenerateTdxMinCode("./data/92kebi_agg_minute.csv", "./data/92kebi_tdx_min.txt")
	// //txcg
	// GenerateCsv("./data/txcg.csv", "./data/txcg_agg_minute.csv", "./data/txcg_agg_day.csv")
	// GenerateTdxDayCode("./data/txcg_agg_day.csv", "./data/txcg_tdx_day.txt")
	// GenerateTdxMinCode("./data/txcg_agg_minute.csv", "./data/txcg_tdx_min.txt")

	//guiyin
	GenerateCsv("./data/guiyin.csv", "./data/guiyin_agg_minute.csv", "./data/guiyin_agg_day.csv")
	GenerateTdxDayCode("./data/guiyin_agg_day.csv", "./data/guiyin_tdx_day.txt", 1)
	GenerateTdxMinCode("./data/guiyin_agg_minute.csv", "./data/guiyin_tdx_min.txt")
}

func GenerateCsv(inputFile, outputmin, outputday string) {
	inputFileName := inputFile

	// 1. 打开原始文件
	file, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("无法打开输入文件: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read() // 跳过表头
	if err != nil {
		log.Fatal("读取表头失败:", err)
	}

	// 准备两个 Map 和两个顺序切片
	minMap := make(map[MinuteKey]*Stats)
	dayMap := make(map[DayKey]*Stats)
	var minKeys []MinuteKey
	var dayKeys []DayKey

	// 2. 一次遍历，双重聚合
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		// 解析基础字段
		date := strings.TrimSpace(row[0])
		timeFull := strings.TrimSpace(row[1])
		code := strings.TrimSpace(row[2])
		name := strings.TrimSpace(row[3])
		direction := strings.TrimSpace(row[4])
		volume, _ := strconv.ParseFloat(strings.TrimSpace(row[6]), 64)
		price, _ := strconv.ParseFloat(strings.TrimSpace(row[5]), 64)
		amount := price * volume

		// 提取分钟 (HH:MM)
		timeParts := strings.Split(timeFull, ":")
		minuteStr := timeFull
		if len(timeParts) >= 2 {
			minuteStr = timeParts[0] + ":" + timeParts[1]
		}

		// --- 处理分钟级聚合 ---
		mKey := MinuteKey{date, minuteStr, code, name, direction}
		if s, exists := minMap[mKey]; exists {
			s.TotalVolume += volume
			s.TotalAmount += amount
		} else {
			minMap[mKey] = &Stats{volume, amount}
			minKeys = append(minKeys, mKey)
		}

		// --- 处理天级聚合 ---
		dKey := DayKey{date, code, name, direction}
		if s, exists := dayMap[dKey]; exists {
			s.TotalVolume += volume
			s.TotalAmount += amount
		} else {
			dayMap[dKey] = &Stats{volume, amount}
			dayKeys = append(dayKeys, dKey)
		}
	}

	// 3. 写入分钟级结果
	writeResult(outputmin, header, func(w *csv.Writer) {
		for _, k := range minKeys {
			s := minMap[k]
			avgPrice := calcWeightedAvg(s.TotalAmount, s.TotalVolume)
			w.Write([]string{
				k.Date, k.Minute + ":00", k.Code, k.Name, k.Direction,
				fmt.Sprintf("%.2f", avgPrice),
				fmt.Sprintf("%.0f", s.TotalVolume),
				fmt.Sprintf("%.2f", s.TotalAmount),
			})
		}
	})

	// 4. 写入天级结果
	dayHeader := header
	dayHeader = append(dayHeader[:1], dayHeader[2:]...) //去掉成交时间这一列
	//dayHeader := []string{"成交日期", "证券代码", "证券名称", "买卖方向", "成交均价", "总成交数量", "总成交金额"}
	writeResult(outputday, dayHeader, func(w *csv.Writer) {
		for _, k := range dayKeys {
			s := dayMap[k]
			avgPrice := calcWeightedAvg(s.TotalAmount, s.TotalVolume)
			w.Write([]string{
				k.Date, k.Code, k.Name, k.Direction,
				fmt.Sprintf("%.2f", avgPrice),
				fmt.Sprintf("%.0f", s.TotalVolume),
				fmt.Sprintf("%.2f", s.TotalAmount),
			})
		}
	})

	fmt.Println("处理完成！已生成两个：")
	fmt.Println("1. agg_minute.csv (分钟级)")
	fmt.Println("2. agg_day.csv (天级)")
}

func GenerateTdxDayCode(inputFile, outputFile string, nSortType int) {
	// 1. 打开传入的 CSV 文件
	//inputFile := "./data/wjwy_day.csv"
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("无法打开文件 %s: %v\n", inputFile, err)
		return
	}
	defer file.Close()

	// 2. 创建用于保存通达信公式的输出文件
	//outputFile := "tdx_formulas.txt"
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("无法创建输出文件: %v\n", err)
		return
	}
	defer outFile.Close()

	nreader := transform.NewReader(file, simplifiedchinese.GB18030.NewDecoder())
	// 3. 创建CSV阅读器并读取所有行
	reader := csv.NewReader(nreader)

	//reader := csv.NewReader(file)

	// 3. 读取并跳过表头
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("读取表头失败: %v\n", err)
		return
	}

	// 4. 读取所有数据行
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("读取数据行失败: %v\n", err)
		return
	}

	if nSortType == 1 { //按代码排序
		sort.Slice(records, func(i, j int) bool {
			// 防止越界检查（可选，如果数据保证规范可省略）
			if len(records[i]) <= 2 || len(records[j]) <= 2 {
				return false
			}
			return records[i][1] < records[j][1]
		})
	} else if nSortType == 2 { //按时间排序
		sort.Slice(records, func(i, j int) bool {
			// 防止越界检查（可选，如果数据保证规范可省略）
			if len(records[i]) <= 2 || len(records[j]) <= 2 {
				return false
			}

			return records[i][0] < records[j][0]
		})
	}

	// 5. 遍历每一行数据进行处理
	for _, row := range records {
		if len(row) < 7 {
			continue // 数据列不足时跳过
		}

		dateStr := row[0]   // 成交日期
		code := row[1]      // 证券代码
		direction := row[3] // 买卖方向
		price := row[4]     // 成交价格
		qtyStr := row[5]    // 成交数量

		// --- 转换日期格式 ---
		// 通达信格式: (年 - 1900) * 10000 + 月 * 100 + 日
		t, err := time.Parse("2006/1/2", dateStr) // Go的特有时间格式化基准时间
		if err != nil {
			t, err = time.Parse("20060102", dateStr)
			if err != nil {
				fmt.Printf("日期解析错误 [%s]: %v\n", dateStr, err)
				continue
			}
		}
		year, month, day := t.Date()
		tdxDate := (year-1900)*10000 + int(month)*100 + day

		// --- 转换数量格式 ---
		// 默认将股数转为“手” (除以 100)
		qty, err := strconv.ParseFloat(qtyStr, 64)
		if err != nil {
			qty = 0
		}
		//lots := int(qty / 100)
		lots := int(qty)

		// --- 生成公式文本 ---
		var formula string
		if strings.Contains(direction, "买入") {
			formula = fmt.Sprintf("DRAWICON(CODE='%s' AND DATE=%d, LOW, 1);\r\nDRAWTEXT(CODE='%s' AND DATE=%d, LOW*0.97,'%s*%d'),COLORRED;\r\n",
				code, tdxDate, code, tdxDate, price, lots)
			// formula = fmt.Sprintf("DRAWTEXT(CODE='%s' AND DATE=%d, LOW*0.97,'买%s*%d'),COLORRED;\r\n\r\n",
			// 	code, tdxDate, price, lots)
		} else if strings.Contains(direction, "卖出") {
			formula = fmt.Sprintf("DRAWICON(CODE='%s' AND DATE=%d, HIGH*1.01, 2);\r\nDRAWTEXT(CODE='%s' AND DATE=%d, HIGH*1.02,'%s*%d'),COLORGREEN;\r\n",
				code, tdxDate, code, tdxDate, price, lots)
			// formula = fmt.Sprintf("DRAWTEXT(CODE='%s' AND DATE=%d, HIGH*1.02,'卖%s*%d'),COLORGREEN;\r\n\r\n",
			// 	 code, tdxDate, price, lots)
		}

		// 写入文件
		if formula != "" {
			_, err = outFile.WriteString(formula)
			if err != nil {
				fmt.Printf("写入文件失败: %v\n", err)
				return
			}
		}
	}

	fmt.Printf("处理完成！通达信日K公式代码已成功生成到文件:%s中。\n", outputFile)
}

func GenerateTdxMinCode(inputFile, outputFile string) {
	// 1. 打开传入的分钟级 CSV 文件
	//inputFile := "./data/wjwy_minute.csv"
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("无法打开文件 %s: %v\n", inputFile, err)
		return
	}
	defer file.Close()

	// 2. 创建用于保存通达信公式的输出文件
	//outputFile := "tdx_minute_formulas.txt"
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("无法创建输出文件: %v\n", err)
		return
	}
	defer outFile.Close()

	nreader := transform.NewReader(file, simplifiedchinese.GB18030.NewDecoder())

	// 3. 创建CSV阅读器并读取所有行
	reader := csv.NewReader(nreader)
	//reader := csv.NewReader(file)

	// 3. 读取并跳过表头
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("读取表头失败: %v\n", err)
		return
	}

	// 4. 读取所有数据行
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("读取数据行失败: %v\n", err)
		return
	}

	sort.Slice(records, func(i, j int) bool {
		// 防止越界检查（可选，如果数据保证规范可省略）
		if len(records[i]) <= 3 || len(records[j]) <= 3 {
			return false
		}
		return records[i][2] < records[j][2]
	})

	// 5. 遍历每一行数据进行处理
	for _, row := range records {
		// 分钟线数据的字段较多，确认长度足够 (至少8列)
		if len(row) < 8 {
			continue
		}

		dateStr := row[0]   // 成交日期
		timeStr := row[1]   // 成交时间 (格式如 9:30:00)
		code := row[2]      // 证券代码
		direction := row[4] // 买卖方向
		price := row[5]     // 成交价格
		qtyStr := row[6]    // 成交数量

		// --- 转换日期格式 ---
		// 通达信格式: (年 - 1900) * 10000 + 月 * 100 + 日
		t, err := time.Parse("2006/1/2", dateStr)
		if err != nil {
			t, err = time.Parse("20060102", dateStr)
			if err != nil {
				fmt.Printf("日期解析错误 [%s]: %v\n", dateStr, err)
				continue
			}
		}
		year, month, day := t.Date()
		tdxDate := (year-1900)*10000 + int(month)*100 + day

		// --- 转换时间格式 ---
		// 通达信格式: 小时*100 + 分钟 (例如 13:25:00 -> 1325, 9:30:00 -> 930)
		timeParts := strings.Split(timeStr, ":")
		if len(timeParts) < 2 {
			continue
		}
		hour, _ := strconv.Atoi(timeParts[0])
		minute, _ := strconv.Atoi(timeParts[1])
		tdxTime := hour*100 + minute

		// --- 转换数量格式 ---
		// 将股数转为“手” (除以 100)
		qty, err := strconv.ParseFloat(qtyStr, 64)
		if err != nil {
			qty = 0
		}
		//lots := int(qty / 100)
		lots := int(qty)

		// --- 生成公式文本 ---
		var formula string
		if strings.Contains(direction, "买入") {
			formula = fmt.Sprintf("DRAWICON(CODE='%s' AND DATE=%d AND TIME=%d, LOW, 1);\r\nDRAWTEXT(CODE='%s' AND DATE=%d AND TIME=%d, LOW*0.99,'%s*%d'),COLORRED;\r\n",
				code, tdxDate, tdxTime, code, tdxDate, tdxTime, price, lots)
		} else if strings.Contains(direction, "卖出") {
			formula = fmt.Sprintf("DRAWICON(CODE='%s' AND DATE=%d AND TIME=%d, HIGH, 2);\r\nDRAWTEXT(CODE='%s' AND DATE=%d AND TIME=%d, HIGH*1.01,'%s*%d'),COLORGREEN;\r\n",
				code, tdxDate, tdxTime, code, tdxDate, tdxTime, price, lots)
		}

		// 写入文件
		if formula != "" {
			_, err = outFile.WriteString(formula)
			if err != nil {
				fmt.Printf("写入文件失败: %v\n", err)
				return
			}
		}
	}

	fmt.Printf("处理完成！通达信分钟图公式已成功生成到文件:%s中。\n", outputFile)
}

// calcWeightedAvg 计算加权平均并四舍五入
func calcWeightedAvg(amount, volume float64) float64 {
	if volume == 0 {
		return 0
	}
	price := amount / volume
	return math.Round(price*100) / 100
}

// writeResult 通用的 CSV 写入函数，自动添加 BOM
func writeResult(fileName string, header []string, writeData func(*csv.Writer)) {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("无法创建文件 %s: %v", fileName, err)
	}
	defer f.Close()

	// 写入 UTF-8 BOM
	//f.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(f)
	defer writer.Flush()

	writer.Write(header)
	writeData(writer)
}
