"""
Email Service — Azure Communication Services (ACS)
Sends transactional emails via the Azure Communication Services Email SDK.

Azure Communication Services is provisioned automatically by Terraform:
  - Communication Service → Email Service → Managed Domain → linked together
  - Connection string passed as ACS_CONNECTION_STRING env var
  - Sender address auto-generated: DoNotReply@<managed-domain>

Environment variables:
  ACS_CONNECTION_STRING  - Azure Communication Services connection string
  ACS_SENDER_ADDRESS     - Sender address (e.g. DoNotReply@<guid>.azurecomm.net)
  EMAIL_ENABLED          - Set to 'true' to enable (dry-run by default)
"""

import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Config ──
ACS_CONNECTION_STRING = os.environ.get('ACS_CONNECTION_STRING', '')
ACS_SENDER_ADDRESS = os.environ.get('ACS_SENDER_ADDRESS', '')
EMAIL_ENABLED = os.environ.get('EMAIL_ENABLED', 'false').lower() == 'true'
SYNAPX_NOTIFICATION_EMAIL = os.environ.get('SYNAPX_NOTIFICATION_EMAIL', '')

FRONTEND_URL = os.environ.get('FRONTEND_URL', 'https://xtractai-dev-frontend-tuo03u.delightfulgrass-29f46ee0.uksouth.azurecontainerapps.io')


def is_email_configured() -> bool:
    """Check if ACS is configured and email sending is enabled."""
    return bool(ACS_CONNECTION_STRING and ACS_SENDER_ADDRESS and EMAIL_ENABLED)


def _send_email(to_email: str, subject: str, html_body: str, text_body: str = '') -> bool:
    """
    Send an email via Azure Communication Services.

    Returns True if sent, False if failed or disabled.
    """
    if not EMAIL_ENABLED:
        logger.info(f"[EMAIL DRY-RUN] Would send '{subject}' to {to_email} (EMAIL_ENABLED=false)")
        return False

    if not ACS_CONNECTION_STRING or not ACS_SENDER_ADDRESS:
        logger.warning(f"ACS not configured — skipping send to {to_email}")
        return False

    try:
        from azure.communication.email import EmailClient

        client = EmailClient.from_connection_string(ACS_CONNECTION_STRING)

        message = {
            "senderAddress": ACS_SENDER_ADDRESS,
            "content": {
                "subject": subject,
                "html": html_body,
            },
            "recipients": {
                "to": [
                    {
                        "address": to_email,
                        "displayName": to_email.split('@')[0],
                    }
                ]
            },
        }

        # Add plain-text fallback if provided
        if text_body:
            message["content"]["plainText"] = text_body

        poller = client.begin_send(message)
        result = poller.result()

        logger.info(f"Email sent via ACS: '{subject}' → {to_email} (messageId={result.get('id', 'n/a')})")
        return True

    except Exception as e:
        logger.error(f"Failed to send email via ACS to {to_email}: {e}", exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════
# WELCOME EMAIL — sent after marketplace purchase
# ══════════════════════════════════════════════════════════════

def send_welcome_email(
    to_email: str,
    buyer_name: str = '',
    plan_name: str = '',
    org_name: str = '',
) -> bool:
    """
    Send the welcome/onboarding email after a successful marketplace purchase.

    Args:
        to_email: Buyer's email address (from marketplace beneficiary)
        buyer_name: Display name of the buyer
        plan_name: Plan they purchased (e.g. 'enterprise')
        org_name: Organization name
    """
    display_name = buyer_name or org_name or 'there'
    webapp_url = FRONTEND_URL
    plan_display = (plan_name or 'your plan').replace('_', ' ').title()

    subject = '🎉 Welcome to Xtract — You\'re all set!'

    html_body = _build_welcome_html(
        display_name=display_name,
        webapp_url=webapp_url,
        plan_display=plan_display,
        org_name=org_name,
    )

    text_body = _build_welcome_text(
        display_name=display_name,
        webapp_url=webapp_url,
        plan_display=plan_display,
    )

    return _send_email(to_email, subject, html_body, text_body)


# ── Plain-text fallback ──

def _build_welcome_text(display_name: str, webapp_url: str, plan_display: str) -> str:
    return f"""Hi {display_name},

Welcome to Xtract — great to have you on board! 🎉

You can get started here: {webapp_url}

Quick setup:
1. Sign in with your Microsoft 365 account
2. Go to Templates (left panel)
3. Click New Template
4. Add a name + description
5. Click Create and Continue
6. Click Upload Documents
7. Upload 3–5 sample documents of the same type
8. Click Suggest Fields
9. Review and tweak the fields (name, type, category, etc.)
10. Click Build Analyzer

That's it — your analyzer is now ready for your team to use!

Plan: {plan_display}

If you get stuck or want a quick demo, just reply to this email
or reach out to us at support@xtract.tech

Best,
Xtract Team
"""


# ── Branded HTML email ──

def _build_welcome_html(
    display_name: str,
    webapp_url: str,
    plan_display: str,
    org_name: str = '',
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Welcome to Xtract</title>
</head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">

  <!-- Wrapper -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f3f4f6;padding:32px 16px;">
    <tr>
      <td align="center">

        <!-- Card -->
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">

          <!-- Header -->
          <tr>
            <td style="background:linear-gradient(135deg,#1e3a5f,#2563eb);padding:32px 40px;text-align:center;">
              <h1 style="margin:0;font-size:28px;font-weight:700;color:#ffffff;letter-spacing:-0.5px;">
                Xtract
              </h1>
              <p style="margin:8px 0 0;font-size:14px;color:rgba(255,255,255,0.8);">
                Intelligent Document Extraction
              </p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:40px;">

              <!-- Greeting -->
              <p style="margin:0 0 8px;font-size:18px;font-weight:600;color:#111827;">
                Hi {display_name},
              </p>
              <p style="margin:0 0 24px;font-size:15px;color:#374151;line-height:1.6;">
                Welcome to Xtract — great to have you on board! 🎉
              </p>

              <!-- Plan badge -->
              <table role="presentation" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
                <tr>
                  <td style="background-color:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 20px;">
                    <span style="font-size:13px;color:#1e40af;">
                      ✅ Your plan: <strong>{plan_display}</strong>
                      {f' &nbsp;·&nbsp; Organisation: <strong>{org_name}</strong>' if org_name else ''}
                    </span>
                  </td>
                </tr>
              </table>

              <!-- CTA Button -->
              <table role="presentation" cellpadding="0" cellspacing="0" style="margin-bottom:32px;" width="100%">
                <tr>
                  <td align="center">
                    <a href="{webapp_url}" target="_blank"
                       style="display:inline-block;padding:14px 36px;background:linear-gradient(135deg,#2563eb,#4f46e5);color:#ffffff;font-size:15px;font-weight:600;text-decoration:none;border-radius:8px;">
                      Open Xtract Web App &rarr;
                    </a>
                  </td>
                </tr>
              </table>

              <!-- Divider -->
              <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 24px;" />

              <!-- Setup Steps -->
              <p style="margin:0 0 16px;font-size:16px;font-weight:600;color:#111827;">
                🚀 Quick Setup Guide
              </p>

              <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:24px;">
                {_step_row(1, '🔑', 'Sign in', 'Use your Microsoft 365 account to sign in.')}
                {_step_row(2, '📋', 'Go to Templates', 'Click <strong>Templates</strong> in the left sidebar.')}
                {_step_row(3, '➕', 'New Template', 'Click <strong>New Template</strong>, add a name and description.')}
                {_step_row(4, '✏️', 'Create & Continue', 'Click <strong>Create and Continue</strong> to open template settings.')}
                {_step_row(5, '📄', 'Upload Documents', 'Click <strong>Upload Documents</strong> and upload 3–5 sample documents of the same type.')}
                {_step_row(6, '✨', 'Suggest Fields', 'Click <strong>Suggest Fields</strong> — our AI will analyse your docs and propose extractable fields.')}
                {_step_row(7, '✅', 'Review Fields', 'Review, rename, re-type, or remove fields as needed.')}
                {_step_row(8, '🤖', 'Build Analyzer', 'Click <strong>Build Analyzer</strong> — your template is now live and ready for your team!')}
              </table>

              <!-- Pro tip -->
              <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:28px;">
                <tr>
                  <td style="background-color:#fefce8;border:1px solid #fde68a;border-radius:8px;padding:14px 18px;">
                    <p style="margin:0;font-size:13px;color:#92400e;line-height:1.5;">
                      💡 <strong>Pro tip:</strong> Upload documents that are the same type
                      (e.g., all insurance policies, or all invoices). The AI works best when the
                      sample docs are similar — it'll discover the most complete set of extractable fields.
                    </p>
                  </td>
                </tr>
              </table>

              <!-- Divider -->
              <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 24px;" />

              <!-- Support -->
              <p style="margin:0 0 4px;font-size:14px;color:#374151;line-height:1.6;">
                If you get stuck or want a quick demo, just reply to this email or
                reach out to us at
                <a href="mailto:support@xtract.tech" style="color:#2563eb;text-decoration:none;font-weight:500;">
                  support@xtract.tech
                </a>
              </p>
              <p style="margin:24px 0 0;font-size:14px;color:#374151;">
                Best,<br />
                <strong>The Xtract Team</strong>
              </p>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background-color:#f9fafb;padding:20px 40px;text-align:center;border-top:1px solid #e5e7eb;">
              <p style="margin:0;font-size:12px;color:#9ca3af;">
                © 2026 Xtract by Synapx · London, UK
              </p>
              <p style="margin:4px 0 0;font-size:12px;color:#9ca3af;">
                <a href="{webapp_url}" style="color:#6b7280;text-decoration:none;">Web App</a>
                &nbsp;·&nbsp;
                <a href="mailto:support@xtract.tech" style="color:#6b7280;text-decoration:none;">Support</a>
              </p>
            </td>
          </tr>

        </table>
        <!-- /Card -->

      </td>
    </tr>
  </table>
  <!-- /Wrapper -->

</body>
</html>"""


def _step_row(num: int, emoji: str, title: str, description: str) -> str:
    """Build a single step row for the HTML email."""
    return f"""<tr>
  <td style="padding:10px 0;vertical-align:top;border-bottom:1px solid #f3f4f6;">
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%">
      <tr>
        <td width="36" style="vertical-align:top;padding-right:12px;">
          <div style="width:32px;height:32px;border-radius:50%;background-color:#eff6ff;text-align:center;line-height:32px;font-size:14px;font-weight:700;color:#2563eb;">
            {num}
          </div>
        </td>
        <td style="vertical-align:top;">
          <p style="margin:0;font-size:14px;font-weight:600;color:#111827;">
            {emoji} {title}
          </p>
          <p style="margin:2px 0 0;font-size:13px;color:#6b7280;line-height:1.4;">
            {description}
          </p>
        </td>
      </tr>
    </table>
  </td>
</tr>"""


# ══════════════════════════════════════════════════════════════
# INTERNAL NOTIFICATION — sent to Synapx team
# ══════════════════════════════════════════════════════════════

def send_internal_notification(
    event_type: str,
    org_name: str = '',
    plan_name: str = '',
    tenant_id: str = '',
    purchaser_email: str = '',
    subscription_id: str = '',
    extra_info: str = '',
) -> bool:
    """
    Send an internal notification email to the Synapx team.

    event_type: 'new_purchase', 'cancellation', 'suspension', 'reinstatement', 'plan_change'
    """
    to_email = SYNAPX_NOTIFICATION_EMAIL
    if not to_email:
        logger.warning("SYNAPX_NOTIFICATION_EMAIL not configured — skipping internal notification")
        return False

    event_labels = {
        'new_purchase': ('🎉 New Customer Purchase', '#059669', 'A new customer has purchased Xtract on Azure Marketplace.'),
        'cancellation': ('🚨 Customer Cancelled', '#dc2626', 'A customer has cancelled their Xtract subscription.'),
        'suspension': ('⚠️ Subscription Suspended', '#d97706', 'A customer subscription has been suspended by Microsoft.'),
        'reinstatement': ('✅ Subscription Reinstated', '#059669', 'A suspended subscription has been reinstated.'),
        'plan_change': ('📋 Plan Changed', '#2563eb', 'A customer has changed their subscription plan.'),
    }

    title, color, description = event_labels.get(
        event_type,
        (f'📣 Marketplace Event: {event_type}', '#6b7280', f'A marketplace event occurred: {event_type}')
    )

    subject = f'[Xtract] {title}'
    plan_display = (plan_name or 'Unknown').replace('_', ' ').title()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M UTC')

    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>{title}</title></head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <tr><td style="background:{color};padding:24px 32px;">
          <h1 style="margin:0;font-size:20px;color:#fff;">{title}</h1>
        </td></tr>
        <tr><td style="padding:32px;">
          <p style="margin:0 0 20px;font-size:15px;color:#374151;line-height:1.6;">{description}</p>
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:24px;">
            <tr><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;"><strong style="color:#6b7280;font-size:13px;">Organisation</strong></td><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:14px;color:#111827;">{org_name or 'N/A'}</td></tr>
            <tr><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;"><strong style="color:#6b7280;font-size:13px;">Plan</strong></td><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:14px;color:#111827;">{plan_display}</td></tr>
            <tr><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;"><strong style="color:#6b7280;font-size:13px;">Purchaser Email</strong></td><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:14px;color:#111827;">{purchaser_email or 'N/A'}</td></tr>
            <tr><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;"><strong style="color:#6b7280;font-size:13px;">Tenant ID</strong></td><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:14px;color:#111827;font-family:monospace;">{tenant_id or 'N/A'}</td></tr>
            <tr><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;"><strong style="color:#6b7280;font-size:13px;">Subscription ID</strong></td><td style="padding:8px 0;border-bottom:1px solid #f3f4f6;font-size:14px;color:#111827;font-family:monospace;">{subscription_id or 'N/A'}</td></tr>
            <tr><td style="padding:8px 0;"><strong style="color:#6b7280;font-size:13px;">Timestamp</strong></td><td style="padding:8px 0;font-size:14px;color:#111827;">{timestamp}</td></tr>
          </table>
          {f'<p style="margin:0;font-size:13px;color:#6b7280;background:#f9fafb;padding:12px;border-radius:6px;">{extra_info}</p>' if extra_info else ''}
        </td></tr>
        <tr><td style="background:#f9fafb;padding:16px 32px;text-align:center;border-top:1px solid #e5e7eb;">
          <p style="margin:0;font-size:12px;color:#9ca3af;">Xtract by Synapx — Internal Notification</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    text_body = f"""{title}

{description}

Organisation: {org_name or 'N/A'}
Plan: {plan_display}
Purchaser Email: {purchaser_email or 'N/A'}
Tenant ID: {tenant_id or 'N/A'}
Subscription ID: {subscription_id or 'N/A'}
Timestamp: {timestamp}
{f'Note: {extra_info}' if extra_info else ''}
"""

    return _send_email(to_email, subject, html_body, text_body)


# ══════════════════════════════════════════════════════════════
# CANCELLATION/FAREWELL EMAIL — sent to customer
# ══════════════════════════════════════════════════════════════

def send_cancellation_email(
    to_email: str,
    buyer_name: str = '',
    org_name: str = '',
    plan_name: str = '',
) -> bool:
    """
    Send a farewell + feedback email to the customer when they cancel.
    """
    display_name = buyer_name or org_name or 'there'
    plan_display = (plan_name or 'your plan').replace('_', ' ').title()

    subject = "We're sorry to see you go — Xtract"

    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>Subscription Cancelled</title></head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <tr><td style="background:linear-gradient(135deg,#1e3a5f,#2563eb);padding:28px 32px;text-align:center;">
          <h1 style="margin:0;font-size:24px;font-weight:700;color:#fff;">Xtract</h1>
          <p style="margin:6px 0 0;font-size:13px;color:rgba(255,255,255,0.8);">Intelligent Document Extraction</p>
        </td></tr>
        <tr><td style="padding:36px;">
          <p style="margin:0 0 8px;font-size:18px;font-weight:600;color:#111827;">Hi {display_name},</p>
          <p style="margin:0 0 20px;font-size:15px;color:#374151;line-height:1.6;">
            We're sorry to see you go. Your Xtract subscription ({plan_display}) has been cancelled.
          </p>
          <p style="margin:0 0 20px;font-size:15px;color:#374151;line-height:1.6;">
            Your data will be retained for 30 days in case you change your mind. After that, it will be permanently deleted.
          </p>

          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom:24px;">
            <tr><td style="background:#fef3c7;border:1px solid #fde68a;border-radius:8px;padding:16px 20px;">
              <p style="margin:0 0 8px;font-size:14px;font-weight:600;color:#92400e;">💬 We'd love your feedback</p>
              <p style="margin:0;font-size:13px;color:#92400e;line-height:1.5;">
                What could we have done better? Was there a feature missing, or was pricing an issue?
                Your feedback helps us improve Xtract for everyone.<br/><br/>
                Just reply to this email — we read every response.
              </p>
            </td></tr>
          </table>

          <p style="margin:0 0 4px;font-size:14px;color:#374151;line-height:1.6;">
            If you'd like to resubscribe in the future, you can do so anytime from the
            <a href="https://azuremarketplace.microsoft.com" style="color:#2563eb;text-decoration:none;font-weight:500;">Azure Marketplace</a>.
          </p>
          <p style="margin:24px 0 0;font-size:14px;color:#374151;">
            All the best,<br/><strong>The Xtract Team</strong>
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:16px 32px;text-align:center;border-top:1px solid #e5e7eb;">
          <p style="margin:0;font-size:12px;color:#9ca3af;">© 2026 Xtract by Synapx · London, UK</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    text_body = f"""Hi {display_name},

We're sorry to see you go. Your Xtract subscription ({plan_display}) has been cancelled.

Your data will be retained for 30 days in case you change your mind. After that, it will be permanently deleted.

We'd love your feedback — what could we have done better? Was there a feature missing, or was pricing an issue? Just reply to this email.

If you'd like to resubscribe in the future, visit the Azure Marketplace: https://azuremarketplace.microsoft.com

All the best,
The Xtract Team
"""

    return _send_email(to_email, subject, html_body, text_body)
