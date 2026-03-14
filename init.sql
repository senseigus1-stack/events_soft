
-- Создание таблицы places (места)
CREATE TABLE IF NOT EXISTS places (
    id BIGINT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    address TEXT,
    description TEXT,
    short_title VARCHAR(100),
    slug VARCHAR(255),
    place_url VARCHAR(500),
    site_url VARCHAR(500),
    image_url VARCHAR(500),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    phone VARCHAR(50),
    timetable TEXT,
    is_free BOOLEAN DEFAULT FALSE,
    is_closed BOOLEAN DEFAULT FALSE,
    disable_comments BOOLEAN DEFAULT FALSE,
    has_parking_lot BOOLEAN DEFAULT FALSE,
    favorites_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    subway TEXT[],
    categories TEXT[],
    tags TEXT[],
    location VARCHAR(50),
    age_restriction VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    city INTEGER,
    status_ml JSONB DEFAULT '[]',
    event_history JSONB DEFAULT '[]',
    referral_code VARCHAR(50) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание таблицы referrals (реферальные связи)
CREATE TABLE IF NOT EXISTS referrals (
    id SERIAL PRIMARY KEY,
    referrer_id BIGINT NOT NULL,
    referred_id BIGINT NOT NULL,
    referral_code VARCHAR(50) NOT NULL,
    is_friend BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT fk_referrer
        FOREIGN KEY (referrer_id)
        REFERENCES users (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_referred
        FOREIGN KEY (referred_id)
        REFERENCES users (id)
        ON DELETE CASCADE,
    CONSTRAINT unique_referral
        UNIQUE (referrer_id, referred_id)
);

-- Создание таблицы user_confirmed_events (подтверждённые события)
CREATE TABLE IF NOT EXISTS user_confirmed_events (
    user_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    confirmed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reminder_sent BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (user_id, event_id),
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
);

-- Создание таблицы user_event_actions (действия пользователей с событиями)
CREATE TABLE IF NOT EXISTS user_event_actions (
    user_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    action VARCHAR(20) NOT NULL,  -- 'like', 'dislike', 'confirmed'
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, event_id, action)
);

-- Индексы для user_event_actions
CREATE INDEX IF NOT EXISTS idx_user_event ON user_event_actions(user_id, event_id);

-- Создание таблицы friends (друзья)
CREATE TABLE IF NOT EXISTS friends (
    user_id BIGINT NOT NULL,
    friend_id BIGINT NOT NULL,
    added_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, friend_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (friend_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Создание таблицы invitations (приглашения на события)
CREATE TABLE IF NOT EXISTS invitations (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    sender_id BIGINT NOT NULL,      -- кто отправил
    receiver_id BIGINT NOT NULL,   -- кому отправили
    token VARCHAR(16) UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL,    -- sent, delivered, viewed, accepted, declined, failed
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Индексы для invitations
CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations(token);
CREATE INDEX IF NOT EXISTS idx_invitations_receiver ON invitations(receiver_id);
CREATE INDEX IF NOT EXISTS idx_invitations_event ON invitations(event_id);

-- Функция для динамического создания таблиц событий для разных городов
CREATE OR REPLACE FUNCTION create_city_event_tables(city_name TEXT)
RETURNS VOID AS $$
DECLARE
    table_name TEXT := LOWER(REPLACE(city_name, '-', '_'));
BEGIN
    -- Проверка корректности имени таблицы
    IF NOT table_name ~ '^[a-z][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'Некорректное имя таблицы: %', table_name;
    END IF;

    -- Создание таблицы событий города
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id BIGINT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            place_name VARCHAR(255),
            address TEXT,
            event_url VARCHAR(500),
            image_url VARCHAR(500),
            start_datetime BIGINT,
            end_datetime BIGINT,
            category VARCHAR(255),
            status VARCHAR(20) DEFAULT ''upcoming'',
            status_ml JSONB,
            publication_date BIGINT,
            slug VARCHAR(255),
            age_restriction VARCHAR(10),
            price VARCHAR(255),
            is_free BOOLEAN,
            tags TEXT[],
            favorites_count INTEGER,
            comments_count INTEGER,
            short_title VARCHAR(255),
            disable_comments BOOLEAN,
            place_id BIGINT,
            likes BIGINT DEFAULT 0,
            added_by BIGINT,
            CONSTRAINT fk_place
                FOREIGN KEY (place_id)
                REFERENCES places (id)
                ON DELETE SET NULL
        )', table_name);

    -- Создание таблицы дат событий
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS event_dates_%I (
            id SERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL,
            start_timestamp BIGINT NOT NULL,
            end_timestamp BIGINT NOT NULL,
            FOREIGN KEY (event_id) REFERENCES %I(id) ON DELETE CASCADE
        )', table_name, table_name);
END;
$$ LANGUAGE plpgsql;

-- Комментарий к функции
COMMENT ON FUNCTION create_city_eventtables(TEXT) IS 'Создаёт таблицы для событий конкретного города: основную таблицу событий и таблицу дат событий';

-- Пример вызова функции для создания таблиц для Москвы и Санкт‑Петербурга
-- Раскомментируйте следующие строки, если хотите сразу создать таблицы для этих городов:
SELECT create_city_eventtables('msk');
SELECT create_city_eventtables('spb');

 
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${DB_USER};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${DB_USER};