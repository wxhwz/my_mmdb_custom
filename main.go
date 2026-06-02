package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang"
)

// ==========================================
// 结构体定义
// ==========================================

// PeeringDBResponse 用于解析 PeeringDB API 响应
type PeeringDBResponse struct {
	Data []struct {
		Asn  uint32 `json:"asn"`
		Name string `json:"name"`
	} `json:"data"`
}

// ASNRecord 用于映射原始 MMDB 中的 ASN 记录
type ASNRecord struct {
	AutonomousSystemNumber       uint32 `maxminddb:"autonomous_system_number"`
	AutonomousSystemOrganization string `maxminddb:"autonomous_system_organization"`
}

// nodeCacheKey 用于缓存 ASN 节点的 Key，降低内存占用和 GC 开销
type nodeCacheKey struct {
	asn uint32
	org string
}

// ==========================================
// City 数据库处理逻辑
// ==========================================

// extractCustomRecord 精准提取指定字段，并将结构拍扁
func extractCustomRecord(record map[string]interface{}) mmdbtype.DataType {
	newRecord := mmdbtype.Map{}
	hasData := false // 用于标记这条记录是不是空的

	// 1. 提取并拍扁 City
	if city, ok := record["city"].(map[string]interface{}); ok {
		if names, ok := city["names"].(map[string]interface{}); ok {
			if enName, ok := names["en"].(string); ok {
				newRecord["city"] = mmdbtype.String(enName)
				hasData = true
			}
		}
	}

	// 2. 提取并拍扁 Country
	if country, ok := record["country"].(map[string]interface{}); ok {
		countryMap := mmdbtype.Map{}
		if isoCode, ok := country["iso_code"].(string); ok {
			countryMap["iso_code"] = mmdbtype.String(isoCode)
			hasData = true
		}
		if names, ok := country["names"].(map[string]interface{}); ok {
			if enName, ok := names["en"].(string); ok {
				countryMap["names"] = mmdbtype.String(enName)
				hasData = true
			}
		}
		if len(countryMap) > 0 {
			newRecord["country"] = countryMap
		}
	}

	// 3. 提取并拍扁 Subdivisions (省份/州)
	if subList, ok := record["subdivisions"].([]interface{}); ok && len(subList) > 0 {
		if sub0, ok := subList[0].(map[string]interface{}); ok {
			if names, ok := sub0["names"].(map[string]interface{}); ok {
				if enName, ok := names["en"].(string); ok {
					newRecord["subdivisions"] = mmdbtype.String(enName)
					hasData = true
				}
			}
		}
	}

	if !hasData {
		return nil
	}
	return newRecord
}

func buildCustomCityDB(inputFile, outputFile string) error {
	log.Printf("正在打开原始 City 数据库: %s\n", inputFile)
	db, err := maxminddb.Open(inputFile)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer db.Close()

	writer, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType: "Custom-GeoIP",
		RecordSize:   int(db.Metadata.RecordSize),
		IPVersion:    int(db.Metadata.IPVersion),
		Languages:    []string{"en"},
		Description:  map[string]string{"en": "Custom Flattened GeoIP Database"},
	})
	if err != nil {
		return fmt.Errorf("创建写入器失败: %w", err)
	}

	log.Println("🚀 正在提取并重构 City 数据结构...")
	networks := db.Networks(maxminddb.SkipAliasedNetworks)
	count, inserted := 0, 0

	for networks.Next() {
		var record map[string]interface{}
		subnet, err := networks.Network(&record)
		if err != nil {
			return fmt.Errorf("读取网络段失败: %w", err)
		}

		customRecord := extractCustomRecord(record)
		if customRecord != nil {
			if err = writer.Insert(subnet, customRecord); err != nil {
				return fmt.Errorf("插入网络段失败: %w", err)
			}
			inserted++
		}

		count++
		if count%1000000 == 0 {
			log.Printf("⏳ City DB 扫描了 %d 个 IP 前缀...", count)
		}
	}

	if networks.Err() != nil {
		return fmt.Errorf("读取网段时发生错误: %w", networks.Err())
	}

	log.Printf("✅ City 处理完毕！扫描前缀: %d 个，实际插入有效前缀: %d 个\n", count, inserted)
	log.Println("💾 正在写入 City DB 到磁盘...")

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer outFile.Close()

	if _, err = writer.WriteTo(outFile); err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}
	log.Printf("🎉 City 极简版数据库生成成功：%s\n", outputFile)
	return nil
}

// ==========================================
// ASN 数据库处理逻辑
// ==========================================

func getPeeringDBMap(cachePath, proxyURL, peeringURL string) (map[uint32]string, error) {
	asnMap := make(map[uint32]string)

	// 1. 尝试从本地文件载入
	if _, err := os.Stat(cachePath); err == nil {
		fmt.Printf("🔄 发现本地缓存，正在从文件载入 PeeringDB 数据: %s\n", cachePath)
		if file, err := os.Open(cachePath); err == nil {
			defer file.Close()
			var data PeeringDBResponse
			if err := json.NewDecoder(file).Decode(&data); err == nil {
				for _, item := range data.Data {
					if item.Asn != 0 && item.Name != "" {
						asnMap[item.Asn] = item.Name
					}
				}
				fmt.Printf("✅ 成功从本地缓存解析了 %d 条 ASN 简短名称。\n", len(asnMap))
				return asnMap, nil
			}
		}
		fmt.Printf("⚠️ 读取本地缓存失败，将尝试重新从网络下载...\n")
	}

	// 2. 从网络下载
	fmt.Println("🌐 本地无有效缓存，正在从 PeeringDB 官方 API 下载最新数据...")

	transport := &http.Transport{}
	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		} else {
			fmt.Printf("⚠️ 代理地址解析失败，将使用直连: %v\n", err)
		}
	} else {
		transport.Proxy = http.ProxyFromEnvironment
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	req, _ := http.NewRequest("GET", peeringURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP 请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("错误的响应状态码: %d", resp.StatusCode)
	}

	var data PeeringDBResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("JSON 解析失败: %w", err)
	}

	for _, item := range data.Data {
		if item.Asn != 0 && item.Name != "" {
			asnMap[item.Asn] = item.Name
		}
	}

	// 保存缓存
	if file, err := os.Create(cachePath); err == nil {
		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(data)
		fmt.Printf("💾 最新数据已成功保存至本地文件: %s\n", cachePath)
	}

	fmt.Printf("✅ 成功从网络加载 %d 条 ASN 简短名称。\n", len(asnMap))
	return asnMap, nil
}

func buildCustomASNDB(originalDB, newDB, cachePath, proxyURL, peeringURL string) error {
	if _, err := os.Stat(originalDB); os.IsNotExist(err) {
		return fmt.Errorf("找不到原始数据库文件: %s", originalDB)
	}

	asnMap, err := getPeeringDBMap(cachePath, proxyURL, peeringURL)
	if err != nil {
		return fmt.Errorf("获取 PeeringDB 数据失败: %w", err)
	}

	fmt.Printf("正在读取原始 ASN 数据库: %s\n", originalDB)
	reader, err := maxminddb.Open(originalDB)
	if err != nil {
		return fmt.Errorf("无法打开原始数据库: %w", err)
	}
	defer reader.Close()

	fmt.Println("🚀 正在将数据写入新的 MMDB 树中...")
	writer, err := mmdbwriter.New(
		mmdbwriter.Options{
			DatabaseType: "GeoLite2-ASN-Custom",
			Languages:    []string{"en"},
			Description:  map[string]string{"en": "Merged GeoLite2-ASN with PeeringDB short names"},
			IPVersion:    6,
		},
	)
	if err != nil {
		return fmt.Errorf("创建 MMDB Writer 失败: %w", err)
	}

	nodeCache := make(map[nodeCacheKey]mmdbtype.Map)
	networks := reader.Networks(maxminddb.SkipAliasedNetworks)

	var record ASNRecord
	insertCount := 0

	for networks.Next() {
		network, err := networks.Network(&record)
		if err != nil {
			continue
		}

		asn := record.AutonomousSystemNumber
		org := record.AutonomousSystemOrganization

		if newOrg, exists := asnMap[asn]; exists {
			org = newOrg
		}

		key := nodeCacheKey{asn: asn, org: org}
		nodeData, exists := nodeCache[key]
		if !exists {
			nodeData = mmdbtype.Map{
				"autonomous_system_number":       mmdbtype.Uint32(asn),
				"autonomous_system_organization": mmdbtype.String(org),
			}
			nodeCache[key] = nodeData
		}

		if err := writer.Insert(network, nodeData); err != nil {
			log.Printf("⚠️ 插入网段 %s 失败: %v", network.String(), err)
		}
		insertCount++
	}

	if networks.Err() != nil {
		return fmt.Errorf("读取原始网段时发生错误: %w", networks.Err())
	}

	fmt.Printf("✅ ASN 原始数据处理完毕，共处理 %d 条网段路由记录。\n", insertCount)

	outFile, err := os.Create(newDB)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer outFile.Close()

	if _, err := writer.WriteTo(outFile); err != nil {
		return fmt.Errorf("写入 MMDB 文件失败: %w", err)
	}

	fmt.Printf("🎉 新的 ASN 数据库已生成: %s\n", newDB)
	return nil
}

// ==========================================
// 下载辅助函数
// ==========================================

// downloadFileIfMissing 若本地文件不存在，则从指定 downloadURL 下载
func downloadFileIfMissing(downloadURL, filePath, proxyURL string) error {
	if _, err := os.Stat(filePath); err == nil {
		log.Printf("✅ 本地文件已存在，无需下载: %s", filePath)
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("检查文件状态失败: %w", err)
	}

	log.Printf("⬇️  本地文件不存在，开始下载: %s -> %s", downloadURL, filePath)

	transport := &http.Transport{}
	if proxyURL != "" {
		proxy, err := url.Parse(proxyURL) // 这里 url 指的是 net/url 包，不再被参数遮蔽
		if err != nil {
			log.Printf("⚠️ 代理地址解析失败，使用直连: %v", err)
		} else {
			transport.Proxy = http.ProxyURL(proxy)
		}
	} else {
		transport.Proxy = http.ProxyFromEnvironment
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second,
	}

	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("下载请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("下载失败，HTTP状态码: %d", resp.StatusCode)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建本地文件失败: %w", err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("写入文件失败: %w", err)
	}

	log.Printf("✅ 下载完成，已缓存至: %s", filePath)
	return nil
}

// ==========================================
// 主函数入口
// ==========================================

func main() {
	// ================= 配置区 =================
	//baseDir := `F:\App_share\tools\mmdbinspect_2.0.0_windows_amd64`
	baseDir := "."
	// City DB 配置（使用 filepath.Join 保证跨平台路径）
	cityInput := filepath.Join(baseDir, "GeoLite2-City.mmdb")
	cityOutput := filepath.Join(baseDir, "GeoLite2-City-Custom.mmdb")

	// ASN DB 配置
	asnInput := filepath.Join(baseDir, "GeoLite2-ASN.mmdb")
	asnOutput := filepath.Join(baseDir, "GeoLite2-ASN-Custom.mmdb")
	asnCache := filepath.Join(baseDir, "peeringdb_net.json")

	// 下载源地址（如果本地缺失则从这些地址下载）
	cityDownloadURL := "https://raw.githubusercontent.com/P3TERX/GeoLite.mmdb/refs/heads/download/GeoLite2-City.mmdb"
	asnDownloadURL := "https://raw.githubusercontent.com/P3TERX/GeoLite.mmdb/refs/heads/download/GeoLite2-ASN.mmdb"

	// 网络与 API 配置
	proxyURL := "" // 如果留空 "" 则自动使用系统环境变量代理
	peeringURL := "https://peeringdb.com/api/net"
	// ==========================================

	start := time.Now()
	fmt.Println("========== 检查并下载所需 MMDB 文件 ==========")

	// 自动下载缺失的原始数据库文件
	if err := downloadFileIfMissing(cityDownloadURL, cityInput, proxyURL); err != nil {
		log.Fatalf("❌ 获取 City 数据库失败: %v", err)
	}
	if err := downloadFileIfMissing(asnDownloadURL, asnInput, proxyURL); err != nil {
		log.Fatalf("❌ 获取 ASN 数据库失败: %v", err)
	}

	fmt.Println("========== 开始处理 City 数据库 ==========")
	if err := buildCustomCityDB(cityInput, cityOutput); err != nil {
		log.Printf("❌ 遇到错误停止处理 City DB: %v\n", err)
	}

	fmt.Println("\n========== 开始处理 ASN 数据库 ==========")
	if err := buildCustomASNDB(asnInput, asnOutput, asnCache, proxyURL, peeringURL); err != nil {
		log.Printf("❌ 遇到错误停止处理 ASN DB: %v\n", err)
	}

	fmt.Printf("\n🏁 全部任务执行完成，总耗时: %v\n", time.Since(start))
}
