import type { Message } from '../features/conversation/conversation-types';
export function MessageBubble({ message }: { message: Message }) { return <div className={`message-row ${message.role}`}><div className="message-bubble">{message.text || '…'}</div></div>; }
