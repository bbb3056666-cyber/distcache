# 分布式缓存演示：启动 3 个节点 + 前端 API
# 用法：在项目根目录执行 .\core\example\run.ps1

Write-Host "== 启动分布式缓存集群 =="

Start-Process go -ArgumentList 'run','./core/example','-port','8001','-api'
Write-Host "节点 8001 (+API) 启动中..."
Start-Sleep -Milliseconds 1500

Start-Process go -ArgumentList 'run','./core/example','-port','8002'
Write-Host "节点 8002 启动中..."
Start-Sleep -Milliseconds 1500

Start-Process go -ArgumentList 'run','./core/example','-port','8003'
Write-Host "节点 8003 启动中..."
Start-Sleep -Milliseconds 1500

Write-Host ""
Write-Host "集群就绪，测试命令："
Write-Host '  curl "http://localhost:9999/api?key=Tom"'
Write-Host '  curl "http://localhost:9999/api?key=Jack"'
Write-Host '  curl "http://localhost:9999/api?key=Nobody"  # 404'
Write-Host ""
Write-Host "停止：关掉弹出的几个 go 窗口，或 Ctrl+C"
