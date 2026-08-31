# coding: utf-8
import os
from datetime import datetime

from flask import flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from ..baseview import BaseView


DEFAULT_UPLOAD_DIR = os.environ.get('SCRAPYDWEB_UPLOAD_DIR', '/data/nfs/upload')
DEFAULT_UPLOAD_PUBLIC_BASE_URL = os.environ.get('SCRAPYDWEB_UPLOAD_PUBLIC_BASE_URL', 'http://10.20.8.116/nfs/upload')


class UploadFilesView(BaseView):
    methods = ['GET', 'POST']

    def __init__(self):
        super(UploadFilesView, self).__init__()
        self.template = 'scrapydweb/upload_files.html'
        self.upload_dir = os.path.abspath(DEFAULT_UPLOAD_DIR)

    def dispatch_request(self, **kwargs):
        if request.method == 'POST':
            action = (request.form.get('action') or 'upload').strip()
            if action == 'delete':
                self.handle_delete()
            else:
                self.handle_upload()
            return redirect(url_for('uploadfiles', node=self.node))
        return render_template(self.template, **self.build_kwargs())

    def handle_upload(self):
        os.makedirs(self.upload_dir, exist_ok=True)
        files = request.files.getlist('files')
        if not files:
            flash('未选择文件', self.WARN)
            return

        saved = 0
        for storage in files:
            if not storage or not storage.filename:
                continue
            filename = secure_filename(storage.filename) or 'upload.bin'
            target = self.get_available_path(filename)
            storage.save(target)
            saved += 1
            flash('已上传：%s' % target, self.INFO)

        if saved == 0:
            flash('未找到可上传文件', self.WARN)

    def handle_delete(self):
        filename = (request.form.get('filename') or '').strip()
        if not filename:
            flash('缺少 filename', self.WARN)
            return

        target = os.path.abspath(os.path.join(self.upload_dir, filename))
        if not target.startswith(self.upload_dir.rstrip(os.sep) + os.sep):
            flash('非法文件路径', self.WARN)
            return
        if not os.path.isfile(target):
            flash('文件不存在：%s' % target, self.WARN)
            return

        os.remove(target)
        flash('已删除：%s' % target, self.INFO)

    def get_available_path(self, filename):
        base, ext = os.path.splitext(filename)
        target = os.path.join(self.upload_dir, filename)
        if not os.path.exists(target):
            return target
        suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(self.upload_dir, '%s_%s%s' % (base, suffix, ext))

    def build_kwargs(self):
        rows = []
        if os.path.isdir(self.upload_dir):
            for name in os.listdir(self.upload_dir):
                path = os.path.join(self.upload_dir, name)
                if not os.path.isfile(path):
                    continue
                st = os.stat(path)
                rows.append(dict(
                    name=name,
                    size=st.st_size,
                    mtime=datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    path=path,
                ))
        rows.sort(key=lambda item: (item['mtime'], item['name']), reverse=True)
        return dict(
            node=self.node,
            upload_dir=self.upload_dir,
            upload_public_base_url=DEFAULT_UPLOAD_PUBLIC_BASE_URL.rstrip('/'),
            rows=rows[:100],
        )
