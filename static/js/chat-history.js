// Модуль унифицированной истории чата для всех страниц приложения
(function (global) { // Оборачиваем код в самовызывающуюся функцию, чтобы не засорять глобальную область
  const placeholder = '<span class="text-secondary">—</span>'; // Плейсхолдер для пустых значений в ячейках

  function buildAvatarLabel(label, avatarUrl) { // Строим подпись с аватаркой или только текстом
    const safeLabel = label || '—'; // Фолбэк на случай отсутствия подписи
    if (avatarUrl) { // Если есть ссылка на аватар
      return `<span class="d-inline-flex align-items-center gap-2"><img src="${avatarUrl}" alt="Аватар" class="avatar-inline" loading="lazy" /> <span>${safeLabel}</span></span>`; // Возвращаем HTML с картинкой и подписью
    } // Конец проверки аватара
    return safeLabel; // Возвращаем только текст, если аватара нет
  } // Конец функции сборки подписи с аватаром

  function buildPeerCell(message, options = {}) { // Строим ячейку чата с учетом настроек
    const chatName = message.peer_title || 'Чат без названия'; // Определяем название чата
    const avatarUrl = message.peer_avatar; // Читаем аватар чата
    const allowLink = options.allowLink !== false; // Разрешаем ссылку по умолчанию
    const hasId = Boolean(message.peer_id); // Проверяем наличие идентификатора чата
    if (allowLink && hasId) { // Если можно строить ссылку и есть ID
      return `<a href="/chat/${message.peer_id}" target="_blank" class="text-decoration-none text-light d-inline-flex align-items-center gap-2">${buildAvatarLabel(chatName, avatarUrl)}</a>`; // Возвращаем ссылку с аватаром и подписью
    } // Конец проверки возможности ссылки
    return buildAvatarLabel(chatName, avatarUrl); // Возвращаем подпись без ссылки
  } // Конец функции сборки ячейки чата

  function buildSenderCell(message, options = {}) { // Строим ячейку отправителя с плашкой бота
    const senderName = message.from_name || 'Неизвестный отправитель'; // Определяем имя отправителя
    const avatarUrl = message.from_avatar; // Читаем аватар отправителя
    const allowLink = options.allowLink !== false; // Разрешаем ссылку по умолчанию
    const showBotBadge = options.showBotBadge !== false; // Разрешаем бейдж бота по умолчанию
    const botBadge = message.is_bot && showBotBadge ? '<span class="sender-badge">Бот</span>' : ''; // Формируем бейдж бота при необходимости
    const baseLabel = buildAvatarLabel(senderName, avatarUrl); // Строим базовую подпись с аватаркой
    const hasId = Boolean(message.from_id); // Проверяем наличие ID отправителя
    if (allowLink && hasId) { // Если можно строить ссылку и есть ID
      return `<div class="d-flex align-items-center gap-2 flex-wrap"><a href="/user/${message.from_id}" target="_blank" class="text-decoration-none text-light d-inline-flex align-items-center gap-2">${baseLabel}</a>${botBadge}</div>`; // Возвращаем подпись со ссылкой на профиль
    } // Конец проверки возможности ссылки
    return `<div class="d-flex align-items-center gap-2 flex-wrap">${baseLabel}${botBadge}</div>`; // Возвращаем подпись без ссылки
  } // Конец функции сборки ячейки отправителя

  function buildReplyPreview(reply, options = {}) { // Собираем компактное или детальное превью ответа
    if (!reply) { // Если ответа нет
      return placeholder; // Возвращаем плейсхолдер
    } // Конец проверки ответа
    const mode = options.mode || 'compact'; // Режим отображения: compact или detailed
    if (mode === 'detailed') { // Ветка детализированного вида для админских таблиц
      if (!reply.id && !reply.text && !reply.from_id) { // Проверяем наличие полезных данных
        return placeholder; // Возвращаем плейсхолдер при пустых данных
      } // Конец проверки содержимого
      const replyId = reply.id ?? '—'; // Определяем ID исходного сообщения
      const replyText = reply.text ? reply.text.slice(0, 120) : 'Без текста'; // Готовим укороченный текст
      const replyAuthorLabel = reply.from_name ? `${reply.from_name} (ID: ${reply.from_id ?? '—'})` : reply.from_id ? `ID: ${reply.from_id}` : 'Автор неизвестен'; // Подпись автора
      const replyAuthorCell = buildAvatarLabel(replyAuthorLabel, reply.from_avatar); // Подпись автора с аватаром
      const peerId = options.peerId; // Берем peer_id для построения ссылки
      const replyLink = reply.id && peerId ? `<a href="https://vk.com/im?sel=${peerId}&msgid=${reply.id}" target="_blank" rel="noopener noreferrer">#${reply.id}</a>` : `#${replyId}`; // Строим ссылку на исходное сообщение
      const replyAttachmentsCount = Array.isArray(reply.attachments) ? reply.attachments.length : 0; // Считаем вложения в ответе
      const replyAttachmentsLabel = replyAttachmentsCount ? `${replyAttachmentsCount} влож.` : 'Без вложений'; // Формируем подпись количества вложений
      return `<div class="d-flex flex-column gap-1"><div class="d-flex align-items-center gap-2">${replyLink}<span class="text-secondary small">${replyAttachmentsLabel}</span></div><div class="text-secondary small">${replyText}</div><div class="text-secondary small">${replyAuthorCell}</div></div>`; // Возвращаем HTML блока ответа
    } // Конец ветки детального режима
    const hasReadableData = reply.text || reply.from_name; // Проверяем наличие человекочитаемых данных
    if (!hasReadableData) { // Если данных нет
      return placeholder; // Возвращаем плейсхолдер
    } // Конец проверки наличия данных
    const replyText = reply.text ? reply.text.slice(0, 120) : 'Без текста'; // Подготавливаем текст ответа
    const replyAuthorName = reply.from_name || 'Автор не указан'; // Определяем имя автора
    const replyBotBadge = reply.is_bot ? '<span class="sender-badge">Бот</span>' : ''; // Готовим бейдж бота, если нужно
    return `<div class="d-flex flex-column gap-1"><div class="text-secondary small">Ответ на предыдущее сообщение</div><div class="text-light small">${replyText}</div><div class="d-flex align-items-center gap-2 text-secondary small">${replyAuthorName}${replyBotBadge}</div></div>`; // Возвращаем компактный блок ответа
  } // Конец функции сборки превью ответа

  function prepareGalleryContent(message, galleryApi, galleryKey) { // Нормализуем вложения и регистрируем их в галерее
    const normalizedAttachments = galleryApi?.extractAttachments ? galleryApi.extractAttachments(message) : []; // Получаем нормализованные вложения
    const normalizedCopyHistory = galleryApi?.extractCopyHistory ? galleryApi.extractCopyHistory(message) : []; // Получаем нормализованные репосты
    if (galleryApi?.registerMessageGallery) { // Проверяем наличие функции регистрации галереи
      galleryApi.registerMessageGallery({ ...message, attachments: normalizedAttachments, copy_history: normalizedCopyHistory }, galleryKey); // Регистрируем вложения в общем модуле галереи
    } // Конец проверки регистрации
    const contentCell = galleryApi?.buildContentCell ? galleryApi.buildContentCell(normalizedAttachments, normalizedCopyHistory, galleryKey) : ''; // Формируем HTML ячейки контента
    return { contentCell, normalizedAttachments, normalizedCopyHistory }; // Возвращаем собранные данные для дальнейшего использования
  } // Конец функции подготовки вложений

  function buildDashboardRow(message, galleryApi, galleryKey, options = {}) { // Строим карточку сообщения для дашборда
    const card = document.createElement('div'); // Создаем контейнер карточки
    card.classList.add('chat-item'); // Добавляем базовый класс карточки
    if (options.isNew) { // Если карточка новая
      card.classList.add('message-row-new'); // Добавляем класс анимации появления
    } // Конец проверки новизны карточки
    if (message.is_deleted) { // Если сообщение удалено
      card.classList.add('message-deleted'); // Подсвечиваем карточку удаленного сообщения
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени или плейсхолдер
    const createdAt = formatDate(message.created_at); // Форматируем время создания записи
    const peerCell = buildPeerCell(message, { allowLink: true }); // Строим подпись чата с возможной ссылкой
    const authorCell = buildSenderCell(message, { allowLink: true, showBotBadge: true }); // Строим подпись отправителя с бейджем бота
    const replyCell = buildReplyPreview(message.reply, { mode: 'compact' }); // Готовим компактное превью ответа
    const hasReply = Boolean(message.reply && (message.reply.text || message.reply.from_name || message.reply.from_id)); // Проверяем, есть ли содержательный ответ
    const { contentCell, normalizedAttachments, normalizedCopyHistory } = prepareGalleryContent(message, galleryApi, galleryKey); // Нормализуем вложения и регистрируем их в галерее
    const hasAttachments = (normalizedAttachments?.length || 0) > 0 || (normalizedCopyHistory?.length || 0) > 0; // Проверяем наличие вложений или репостов
    const textCell = message.text ?? ''; // Берем текст сообщения или оставляем пустую строку
    card.innerHTML = `
      <div class="chat-avatar-col">${buildAvatarLabel(message.from_name || message.peer_title, message.from_avatar || message.peer_avatar)}</div>
      <div class="chat-bubble">
        <div class="chat-bubble-header">
          <div class="chat-bubble-meta">${peerCell}</div>
          <div class="chat-bubble-time text-secondary">${createdAt}</div>
        </div>
        <div class="chat-bubble-author">${authorCell}</div>
        ${hasReply ? `<div class="chat-bubble-reply">${replyCell}</div>` : ''}
        ${hasAttachments ? `<div class="chat-bubble-attachments">${contentCell}</div>` : ''}
        <div class="chat-bubble-text">${textCell || placeholder}</div>
      </div>
    `; // Заполняем HTML карточки с метаданными, ответом, вложениями и текстом в стиле мессенджера
    return card; // Возвращаем готовую карточку
  } // Конец функции построения карточки дашборда

  function buildLogsRow(log, galleryApi, galleryKey, options = {}) { // Строим карточку логов в стиле переписки
    const card = document.createElement('div'); // Создаем контейнер карточки
    card.classList.add('chat-item'); // Добавляем базовый класс карточки
    if (log.is_deleted) { // Если запись удалена
      card.classList.add('message-deleted'); // Подсвечиваем карточку как удаленную
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени
    const createdAt = formatDate(log.created_at); // Форматируем время создания
    const peerCell = buildPeerCell(log, { allowLink: true }); // Готовим подпись чата
    const authorCell = buildSenderCell(log, { allowLink: true, showBotBadge: true }); // Готовим подпись отправителя
    const botBadge = log.is_bot ? 'Да' : 'Нет'; // Определяем флаг бота текстом
    const replyCell = buildReplyPreview(log.reply, { mode: 'detailed', peerId: log.peer_id }); // Собираем детальное превью ответа
    const hasReply = Boolean(log.reply && (log.reply.text || log.reply.from_name || log.reply.from_id)); // Проверяем, есть ли содержательный ответ
    const { contentCell, normalizedAttachments, normalizedCopyHistory } = prepareGalleryContent(log, galleryApi, galleryKey); // Подготавливаем вложения
    const hasAttachments = (normalizedAttachments?.length || 0) > 0 || (normalizedCopyHistory?.length || 0) > 0; // Проверяем наличие вложений
    const textCell = log.text ?? ''; // Берем текст сообщения
    const messageIdCell = log.message_id ?? '—'; // Берем ID сообщения VK
    const logIdCell = log.id ?? '—'; // Берем ID записи в базе
    const deleteButton = `<button type="button" class="btn btn-sm btn-outline-danger delete-log-btn" data-log-id="${log.id}">Удалить</button>`; // Формируем кнопку удаления
    card.innerHTML = `
      <div class="chat-avatar-col">${buildAvatarLabel(log.from_name || log.peer_title, log.from_avatar || log.peer_avatar)}</div>
      <div class="chat-bubble"> 
        <div class="chat-bubble-header">
          <div class="chat-bubble-meta">${peerCell}</div>
          <div class="chat-bubble-time text-secondary">${createdAt}</div>
        </div>
        <div class="chat-bubble-author">${authorCell}</div>
        <div class="chat-bubble-flags">Бот: ${botBadge}</div>
        ${hasReply ? `<div class="chat-bubble-reply">${replyCell}</div>` : ''}
        ${hasAttachments ? `<div class="chat-bubble-attachments">${contentCell}</div>` : ''}
        <div class="chat-bubble-text">${textCell || placeholder}</div>
        <div class="chat-bubble-footer">
          <span class="badge bg-dark">VK ID: ${messageIdCell}</span>
          <span class="badge bg-secondary">Запись: ${logIdCell}</span> 
          ${deleteButton} 
        </div> 
      </div>
    `; // Заполняем карточку всеми метаданными, текстом и кнопкой удаления
    return card; // Возвращаем готовую карточку
  } // Конец функции построения карточки логов

  function buildEntityRow(message, galleryApi, galleryKey, options = {}) { // Строим карточку для профилей чатов и пользователей
    const card = document.createElement('div'); // Создаем контейнер карточки
    card.classList.add('chat-item'); // Добавляем базовый класс карточки
    if (message.is_deleted) { // Если сообщение удалено
      card.classList.add('message-deleted'); // Подсвечиваем удаленные сообщения
    } // Конец проверки удаления
    const formatDate = options.formatDate || ((value) => value || '—'); // Берем функцию форматирования времени
    const showChatColumn = Boolean(options.showChatColumn); // Определяем, нужно ли показывать название чата
    const showSenderColumn = Boolean(options.showSenderColumn); // Определяем, нужно ли показывать автора
    const createdAtCell = formatDate(message.created_at); // Форматируем дату создания
    const peerCell = buildPeerCell(message, { allowLink: true }); // Строим подпись чата
    const authorCell = buildSenderCell(message, { allowLink: true, showBotBadge: true }); // Строим подпись отправителя
    const { contentCell, normalizedAttachments, normalizedCopyHistory } = prepareGalleryContent(message, galleryApi, galleryKey); // Подготавливаем вложения и галерею
    const hasAttachments = (normalizedAttachments?.length || 0) > 0 || (normalizedCopyHistory?.length || 0) > 0; // Проверяем наличие вложений или репостов
    const textCell = message.text ?? '—'; // Берем текст сообщения или плейсхолдер
    const metaChunks = [`<span class="chat-bubble-time text-secondary">${createdAtCell}</span>`]; // Создаем массив метаданных, начиная со времени
    if (showChatColumn) { // Если нужно показать чат
      metaChunks.push(`<span class="chat-bubble-meta">${peerCell}</span>`); // Добавляем название чата
    } // Конец проверки чата
    if (showSenderColumn) { // Если нужно показать автора
      metaChunks.push(`<span class="chat-bubble-author">${authorCell}</span>`); // Добавляем подпись отправителя
    } // Конец проверки автора
    card.innerHTML = `
      <div class="chat-avatar-col">${buildAvatarLabel(message.from_name || message.peer_title, message.from_avatar || message.peer_avatar)}</div>
      <div class="chat-bubble">
        <div class="chat-bubble-header">${metaChunks.join(' · ')}</div>
        ${hasAttachments ? `<div class="chat-bubble-attachments">${contentCell}</div>` : ''}
        <div class="chat-bubble-text">${textCell}</div>
      </div>
    `; // Собираем карточку профиля с метаданными, вложениями и текстом
    return card; // Возвращаем готовую карточку
  } // Конец функции построения карточки профиля

  function registerEntityGalleries(messages, galleryApi) { // Регистрируем галереи на страницах профиля чата/пользователя
    if (!Array.isArray(messages)) { // Проверяем, что данные корректные
      return; // Прерываемся, если нет массива
    } // Конец проверки формата
    messages.forEach((msg, idx) => { // Перебираем каждое сообщение
      const galleryKey = `entity-msg-${idx}`; // Формируем ключ галереи по индексу
      prepareGalleryContent(msg, galleryApi, galleryKey); // Регистрируем вложения в модуле галереи
    }); // Конец перебора сообщений
  } // Конец функции регистрации галерей профиля

  global.chatHistory = { // Экспортируем публичное API
    buildAvatarLabel, // Выносим сборку подписи с аватаром
    buildPeerCell, // Выносим сборку ячейки чата
    buildSenderCell, // Выносим сборку ячейки отправителя
    buildReplyPreview, // Выносим превью ответа
    prepareGalleryContent, // Выносим подготовку вложений и регистрацию галерей
    buildDashboardRow, // Выносим построение строки дашборда
    buildLogsRow, // Выносим построение строки логов
    buildEntityRow, // Выносим построение строки профиля чата или пользователя
    registerEntityGalleries, // Выносим регистрацию галерей в профилях
  }; // Завершаем экспорт
})(window); // Передаем window как глобальный объект
