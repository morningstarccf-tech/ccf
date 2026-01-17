(function () {
  function findRow(fieldName) {
    if (!fieldName) return null;
    var el = document.getElementById('id_' + fieldName);
    if (!el) return null;
    return el.closest('.form-row') || el.closest('.fieldBox') || el.parentElement;
  }

  function setRowVisible(fieldName, visible) {
    var row = findRow(fieldName);
    if (!row) return;
    row.style.display = visible ? '' : 'none';
  }

  function setLabelText(fieldName, text) {
    var el = document.getElementById('id_' + fieldName);
    if (!el) return;
    var label = document.querySelector('label[for="id_' + fieldName + '"]');
    if (label) {
      label.textContent = text;
    }
  }

  function updateStorageFields() {
    var targetEl = document.getElementById('id_storage_target');
    var target = targetEl ? targetEl.value : 'default';
    var isDefault = target === 'default';
    var isMysqlHost = target === 'mysql_host';
    var isRemote = target === 'remote_server';
    var isOss = target === 'oss';

    setRowVisible('storage_path', false);

    setRowVisible('remote_storage_path', isMysqlHost || isRemote);
    setRowVisible('remote_protocol', isRemote);
    setRowVisible('remote_host', isRemote);
    setRowVisible('remote_port', isRemote);
    setRowVisible('remote_user', isRemote);
    setRowVisible('remote_password', isRemote);
    setRowVisible('remote_key_path', isRemote);

    setRowVisible('oss_endpoint', isOss);
    setRowVisible('oss_access_key_id', isOss);
    setRowVisible('oss_access_key_secret', isOss);
    setRowVisible('oss_bucket', isOss);
    setRowVisible('oss_prefix', isOss);

    if (isMysqlHost) {
      setLabelText('remote_storage_path', 'MySQL 服务器存储路径');
    } else if (isRemote) {
      setLabelText('remote_storage_path', '远程服务器存储路径');
    }

    var hint = document.getElementById('default-storage-hint');
    if (hint) {
      hint.style.display = isDefault ? '' : 'none';
    }
  }

  function updateScheduleFields() {
    var typeEl = document.getElementById('id_schedule_type');
    if (!typeEl) return;
    var type = typeEl.value || 'daily';

    var showTime = type === 'daily' || type === 'weekly' || type === 'monthly';
    var showWeekday = type === 'weekly';
    var showDay = type === 'monthly';
    var showMinute = type === 'hourly';
    var showEveryMinutes = type === 'every_minutes';

    setRowVisible('schedule_time', showTime);
    setRowVisible('schedule_weekday', showWeekday);
    setRowVisible('schedule_day', showDay);
    setRowVisible('schedule_minute', showMinute);
    setRowVisible('schedule_every_minutes', showEveryMinutes);
  }

  function bindTestButton() {
    var btn = document.getElementById('test-remote-connection');
    if (!btn) return;
    btn.addEventListener('click', function () {
      var url = btn.getAttribute('data-test-url');
      if (!url) return;
      var form = btn.closest('form') || document.querySelector('form');
      if (!form) return;

      var formData = new FormData(form);
      var targetEl = document.getElementById('id_storage_target');
      if (targetEl) {
        formData.set('storage_target', targetEl.value || 'default');
      }

      fetch(url, {
        method: 'POST',
        body: formData,
        headers: {
          'X-Requested-With': 'XMLHttpRequest'
        }
      })
        .then(function (res) { return res.json(); })
        .then(function (data) {
          var message = data && data.message ? data.message : '未知响应';
          alert((data && data.success ? '成功：' : '失败：') + message);
        })
        .catch(function (err) {
          alert('请求失败：' + err);
        });
    });
  }

  function init() {
    updateStorageFields();
    updateScheduleFields();

    var targetEl = document.getElementById('id_storage_target');
    if (targetEl) {
      targetEl.addEventListener('change', updateStorageFields);
    }
    var typeEl = document.getElementById('id_schedule_type');
    if (typeEl) {
      typeEl.addEventListener('change', updateScheduleFields);
    }

    bindTestButton();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
