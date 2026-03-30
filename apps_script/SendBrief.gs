/**
 * Deploy as a Web App so GitHub Actions can call it.
 *
 * 1) Apps Script → Deploy → New deployment → "Web app"
 * 2) Execute as: Me
 * 3) Who has access: Anyone
 * 4) Copy the Web App URL and set it as BRIEF_APPS_SCRIPT_URL in GitHub secrets.
 *
 * Security: set script property BRIEF_SHARED_SECRET and require callers to provide it.
 */

function doPost(e) {
  try {
    var props = PropertiesService.getScriptProperties();
    var expected = props.getProperty('BRIEF_SHARED_SECRET') || '';

    var body = (e && e.postData && e.postData.contents) ? e.postData.contents : '';
    var data = body ? JSON.parse(body) : {};

    var provided = (data && data.secret) ? String(data.secret) : '';
    if (!expected || provided !== expected) {
      return _json(401, { ok: false, error: 'unauthorized' });
    }

    var to = (data && data.to) ? String(data.to) : '';
    if (!to) {
      return _json(400, { ok: false, error: 'missing_to' });
    }

    var subject = (data && data.subject) ? String(data.subject) : 'Executive Partner Brief';
    var htmlBody = (data && data.html) ? String(data.html) : '';
    if (!htmlBody) {
      return _json(400, { ok: false, error: 'missing_html' });
    }

    GmailApp.sendEmail(to, subject, 'Your email client does not support HTML.', {
      htmlBody: htmlBody,
      name: 'Executive Partner Brief'
    });

    return _json(200, { ok: true });
  } catch (err) {
    return _json(500, { ok: false, error: String(err && err.message ? err.message : err) });
  }
}

function doGet() {
  return _json(200, { ok: true, message: 'SendBrief is running.' });
}

function _json(status, obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

