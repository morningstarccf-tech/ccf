from django.shortcuts import render


def index(request):
    # 返回单页 Web UI 入口。
    return render(request, "webui/index.html")
