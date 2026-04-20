import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

from app.konstant import load_mail_data


class Mailer:
    def __init__(self):
        try:
            cfg = load_mail_data()
        except FileNotFoundError:
            cfg = {}

        self.server = cfg.get("server", "localhost")
        self.port = cfg.get("port", 25)
        self.sender = cfg.get("sender", "noreply@example.com")

        self.recipients = self._list(cfg.get("recipients"))
        self.dev_recipients = self._list(cfg.get("dev_recipients"))

        self.cc = self._list(cfg.get("cc"))
        self.bcc = self._list(cfg.get("bcc"))

        self.send_enabled = cfg.get("send_mail", True)

    # -----------------------
    # PUBLIC API
    # -----------------------

    def send(self, subject, body_html=None, attachments=None, dev=True):
        if not self.send_enabled:
            return

        msg = self._build_msg(subject, body_html, attachments, dev)
        self._dispatch(msg, dev)

    def start(self, program, data=None, dev=True):
        codes = ', '.join(map(str, data)) if data else "N/A"
        time = datetime.now().strftime("%Y-%m-%d %H:%M")

        html = f"""
        <p>Program <b>{program}</b> started.</p>
        <p><b>Time:</b> {time}</p>
        <p><b>Codes:</b> {codes}</p>
        """
        self.send(f"[STARTED] {program}", html, dev=dev)

    def end(self, program, dev=False):
        time = datetime.now().strftime("%Y-%m-%d %H:%M")

        html = f"""
        <p>Program <b>{program}</b> completed.</p>
        <p><b>Time:</b> {time}</p>
        """
        self.send(f"[COMPLETED] {program}", html, dev=dev)

    def error(self, program, err=None, dev=True):
        trace = f"<pre>{traceback.format_exc()}</pre>" if err else ""

        html = f"""
        <p><b>{program}</b> failed.</p>
        <p><b>Error:</b> {err}</p>
        {trace}
        """
        self.send(f"[ERROR] {program}", html, dev=dev)

    # -----------------------
    # INTERNALS
    # -----------------------

    def _build_msg(self, subject, body_html, attachments, dev):
        msg = MIMEMultipart("alternative")

        recpts = self.dev_recipients if dev else self.recipients

        msg["From"] = self.sender
        msg["To"] = ", ".join(recpts)
        msg["Subject"] = f"{subject} - {datetime.now().date()}"

        if not dev and self.cc:
            msg["Cc"] = ", ".join(self.cc)

        msg.attach(MIMEText(body_html or "<p>No content</p>", "html"))

        for path in self._list(attachments):
            p = Path(path)
            if p.exists():
                with open(p, "rb") as f:
                    part = MIMEApplication(f.read(), Name=p.name)
                    part['Content-Disposition'] = f'attachment; filename="{p.name}"'
                    msg.attach(part)

        return msg

    def _dispatch(self, msg, dev):
        recpts = self.dev_recipients if dev else self.recipients
        all_recipients = recpts + self.cc + self.bcc

        with smtplib.SMTP(self.server, self.port) as s:
            s.send_message(msg, from_addr=self.sender, to_addrs=all_recipients)

    def _list(self, val):
        if not val:
            return []
        return val if isinstance(val, list) else [val]