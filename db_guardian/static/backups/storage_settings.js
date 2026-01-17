(function () {
  function getRow(fieldId) {
    var el = document.getElementById(fieldId);
    if (!el) {
      return null;
    }
    return el.closest('.form-row') || el.closest('.fieldBox') || el.parentElement;
  }

  function setVisible(fieldId, show) {
    var row = getRow(fieldId);
    if (!row) {
      return;
    }
    row.style.display = show ? '' : 'none';
  }

  function updateLabel(text) {
    var label = document.querySelector("label[for='id_remote_storage_path']");
    if (label) {
      label.textContent = text;
    }
  }

  function toggleFields() {
    var target = document.getElementById('id_storage_target');
    if (!target) {
      return;
    }
    var value = target.value;

    var remoteCommon = ['id_remote_storage_path'];
    var remoteFields = [
      'id_remote_protocol',
      'id_remote_host',
      'id_remote_port',
      'id_remote_user',
      'id_remote_password',
      'id_remote_key_path'
    ];
    var ossFields = [
      'id_oss_endpoint',
      'id_oss_access_key_id',
      'id_oss_access_key_secret',
      'id_oss_bucket',
      'id_oss_prefix'
    ];

    var hint = document.getElementById('default-storage-hint');

    if (value === 'default') {
      remoteCommon.forEach(function (id) { setVisible(id, false); });
      remoteFields.forEach(function (id) { setVisible(id, false); });
      ossFields.forEach(function (id) { setVisible(id, false); });
      updateLabel('存储路径');
      if (hint) {
        hint.style.display = '';
      }
      return;
    }

    if (hint) {
      hint.style.display = 'none';
    }

    if (value === 'mysql_host') {
      remoteCommon.forEach(function (id) { setVisible(id, true); });
      remoteFields.forEach(function (id) { setVisible(id, false); });
      ossFields.forEach(function (id) { setVisible(id, false); });
      updateLabel('MySQL 服务器路径');
      return;
    }

    if (value === 'remote_server') {
      remoteCommon.forEach(function (id) { setVisible(id, true); });
      remoteFields.forEach(function (id) { setVisible(id, true); });
      ossFields.forEach(function (id) { setVisible(id, false); });
      updateLabel('远程服务器路径');
      return;
    }

    if (value === 'oss') {
      remoteCommon.forEach(function (id) { setVisible(id, false); });
      remoteFields.forEach(function (id) { setVisible(id, false); });
      ossFields.forEach(function (id) { setVisible(id, true); });
      updateLabel('存储路径');
      return;
    }
  }

  function getCsrfToken(form) {
    var input = form.querySelector('input[name=csrfmiddlewaretoken]');
    return input ? input.value : '';
  }

  function bindTestButton() {
    var btn = document.getElementById('test-remote-connection');
    if (!btn) {
      return;
    }
    btn.addEventListener('click', function () {
      var form = btn.closest('form');
      var url = btn.getAttribute('data-test-url');
      if (!form || !url) {
        alert('测试接口未配置');
        return;
      }
      var formData = new FormData(form);
      fetch(url, {
        method: 'POST',
        headers: {
          'X-CSRFToken': getCsrfToken(form)
        },
        body: formData
      })
        .then(function (res) { return res.json(); })
        .then(function (data) {
          if (data.success) {
            alert('连通性测试成功');
          } else {
            alert('连通性测试失败: ' + (data.message || '未知错误'));
          }
        })
        .catch(function (err) {
          alert('连通性测试异常: ' + err);
        });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    var target = document.getElementById('id_storage_target');
    if (target) {
      target.addEventListener('change', toggleFields);
    }
    toggleFields();
    bindTestButton();
  });
})();
